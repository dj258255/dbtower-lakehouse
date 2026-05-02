"""데이터 품질 게이트 — 다운스트림(dbt) 앞에 세우는 fail-closed 검문소.

조용히 틀린 데이터는 없는 것보다 나쁘다. raw가 반쪽만 적재됐는데 그 위에 마트를
만들면 "악화 쿼리 랭킹"이 조용히 오답을 낸다. 이 모듈은 dt 파티션이 다운스트림에
넘어가기 전에 세 가지를 검문한다.

  1) reconciliation(정합)  — 원천 PG 행수 == parquet 행수. 인스턴스별로 대조.
                             하나라도 어긋나면 FAIL(적재 유실·중복·부분 적재 탐지).
  2) completeness(완결성)  — 레지스트리(database_instance)의 기대 인스턴스가
                             그 dt 파티션에 전부 존재하는가. 빠지면 FAIL(수집 누락).
  3) freshness(신선도)     — 그 dt의 최신 captured_at이 하루 경계(다음날 00:00)에
                             충분히 근접한가. 임계 초과 시 WARN, 심하면 FAIL
                             (수집이 하루 중간에 끊긴 반쪽 파티션 탐지).

FAIL이 하나라도 있으면 게이트는 '차단'을 반환한다. 오케스트레이터(run_pipeline /
Airflow DAG)는 이 결과로 dbt 실행 여부를 결정한다 — FAIL이면 변환을 돌리지 않는다.

verify_count(Phase 1)의 원천-적재 대조 로직을 흡수·확장했다.

    python -m extract.quality 2026-07-05 2026-07-06 2026-07-07
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import duckdb
import psycopg2

from extract.config import RAW_PREFIX, SinkConfig, SourceConfig

# freshness 임계: dt의 최신 captured_at이 다음날 00:00에서 이만큼 이상 벌어지면 경보.
# WARN = 수집이 늦게 끊겼을 수 있음(경고, 차단 안 함). FAIL = 하루 절반 이상 비었음(차단).
FRESHNESS_WARN_HOURS = float(os.getenv("QUALITY_FRESHNESS_WARN_HOURS", "3"))
FRESHNESS_FAIL_HOURS = float(os.getenv("QUALITY_FRESHNESS_FAIL_HOURS", "12"))

OK, WARN, FAIL = "OK", "WARN", "FAIL"


@dataclass
class CheckResult:
    name: str
    status: str  # OK / WARN / FAIL
    detail: str


@dataclass
class DtReport:
    dt: str
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def status(self) -> str:
        if any(c.status == FAIL for c in self.checks):
            return FAIL
        if any(c.status == WARN for c in self.checks):
            return WARN
        return OK

    @property
    def blocked(self) -> bool:
        # fail-closed: FAIL이 하나라도 있으면 다운스트림을 막는다. WARN은 통과(경고만).
        return self.status == FAIL


def _duck(sink: SinkConfig) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    host = sink.endpoint.replace("http://", "").replace("https://", "")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{host}';")
    con.execute(f"SET s3_access_key_id='{sink.access_key}';")
    con.execute(f"SET s3_secret_access_key='{sink.secret_key}';")
    con.execute("SET s3_use_ssl=false; SET s3_url_style='path';")
    return con


def _registry_instances(src: SourceConfig) -> list[int]:
    with psycopg2.connect(src.dsn()) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM database_instance ORDER BY id")
        return [r[0] for r in cur.fetchall()]


def _pg_counts(src: SourceConfig, dt: str) -> dict[int, int]:
    """원천 PG의 dt 하루창(UTC 반열림) 인스턴스별 행수."""
    day_start = datetime.strptime(dt, "%Y-%m-%d")
    day_end = day_start + timedelta(days=1)
    with psycopg2.connect(src.dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT instance_id, count(*) FROM query_snapshot "
            "WHERE captured_at >= %s AND captured_at < %s GROUP BY instance_id",
            (day_start, day_end),
        )
        return {int(r[0]): int(r[1]) for r in cur.fetchall()}


def _parquet_counts(con: duckdb.DuckDBPyConnection, sink: SinkConfig, dt: str) -> dict[int, int]:
    """적재된 parquet의 dt 파티션 인스턴스별 행수. 파티션이 통째로 없으면 빈 dict."""
    glob = f"s3://{sink.bucket}/{RAW_PREFIX}/dt={dt}/instance_id=*/*.parquet"
    try:
        rows = con.execute(
            f"SELECT instance_id, count(*) FROM read_parquet('{glob}', hive_partitioning = 1) "
            "GROUP BY instance_id"
        ).fetchall()
    except duckdb.IOException:
        # glob이 아무 파일과도 안 맞으면 IOException — 파티션 전무로 간주.
        return {}
    return {int(r[0]): int(r[1]) for r in rows}


def _parquet_max_captured(con: duckdb.DuckDBPyConnection, sink: SinkConfig, dt: str):
    glob = f"s3://{sink.bucket}/{RAW_PREFIX}/dt={dt}/instance_id=*/*.parquet"
    try:
        row = con.execute(
            f"SELECT max(captured_at) FROM read_parquet('{glob}', hive_partitioning = 1)"
        ).fetchone()
    except duckdb.IOException:
        return None
    return row[0] if row else None


def check_reconciliation(pg: dict[int, int], pq: dict[int, int]) -> CheckResult:
    """원천 PG == parquet 행수(인스턴스별). 하나라도 어긋나면 FAIL."""
    instances = sorted(set(pg) | set(pq))
    mismatches = []
    for iid in instances:
        p, q = pg.get(iid, 0), pq.get(iid, 0)
        if p != q:
            mismatches.append(f"inst {iid}: PG {p:,} != parquet {q:,}")
    total_pg, total_pq = sum(pg.values()), sum(pq.values())
    if mismatches:
        return CheckResult(
            "reconciliation", FAIL,
            f"총 PG {total_pg:,} vs parquet {total_pq:,} — " + "; ".join(mismatches),
        )
    return CheckResult("reconciliation", OK, f"PG=parquet={total_pg:,}행 ({len(instances)}인스턴스)")


def check_completeness(registry: list[int], pq: dict[int, int]) -> CheckResult:
    """레지스트리 기대 인스턴스가 파티션에 전부 존재하는가. 빠지면 FAIL."""
    present = {iid for iid, n in pq.items() if n > 0}
    missing = [iid for iid in registry if iid not in present]
    if missing:
        return CheckResult(
            "completeness", FAIL,
            f"기대 {len(registry)}인스턴스 중 누락 {missing} (존재 {sorted(present)})",
        )
    return CheckResult("completeness", OK, f"기대 {len(registry)}인스턴스 전부 존재")


def check_freshness(max_captured, dt: str) -> CheckResult:
    """dt 최신 captured_at이 다음날 00:00에 근접했는가(수집이 중간에 안 끊겼나)."""
    day_end = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    if max_captured is None:
        return CheckResult("freshness", FAIL, "파티션에 데이터 없음(최신 captured_at 없음)")
    if isinstance(max_captured, str):
        max_captured = datetime.fromisoformat(max_captured)
    gap_h = (day_end - max_captured).total_seconds() / 3600.0
    detail = f"최신 {max_captured:%H:%M:%S}, 경계까지 {gap_h:.1f}h"
    if gap_h > FRESHNESS_FAIL_HOURS:
        return CheckResult("freshness", FAIL, detail + f" (>{FRESHNESS_FAIL_HOURS:.0f}h 차단)")
    if gap_h > FRESHNESS_WARN_HOURS:
        return CheckResult("freshness", WARN, detail + f" (>{FRESHNESS_WARN_HOURS:.0f}h 경고)")
    return CheckResult("freshness", OK, detail)


def evaluate(days: list[str]) -> list[DtReport]:
    src, sink = SourceConfig(), SinkConfig()
    registry = _registry_instances(src)
    con = _duck(sink)
    reports: list[DtReport] = []
    for dt in days:
        pg = _pg_counts(src, dt)
        pq = _parquet_counts(con, sink, dt)
        mx = _parquet_max_captured(con, sink, dt)
        rep = DtReport(dt=dt)
        rep.checks.append(check_reconciliation(pg, pq))
        rep.checks.append(check_completeness(registry, pq))
        rep.checks.append(check_freshness(mx, dt))
        reports.append(rep)
    return reports


def print_report(reports: list[DtReport]) -> None:
    print(f"{'dt':<12} {'check':<15} {'status':<7} detail")
    print("-" * 88)
    for rep in reports:
        for i, c in enumerate(rep.checks):
            dt = rep.dt if i == 0 else ""
            print(f"{dt:<12} {c.name:<15} {c.status:<7} {c.detail}")
        print(f"{'':<12} {'= dt verdict':<15} {rep.status:<7} "
              f"{'다운스트림 차단' if rep.blocked else '통과'}")
        print("-" * 88)
    blocked = [r.dt for r in reports if r.blocked]
    if blocked:
        print(f"GATE: BLOCKED — FAIL 파티션 {blocked} → dbt 미실행(fail-closed)")
    else:
        print("GATE: PASS — 모든 dt 통과 → 다운스트림 진행 가능")


def assert_gate(days: list[str]) -> list[DtReport]:
    """게이트를 돌리고, FAIL이 있으면 예외를 던진다(Airflow/스크립트 fail-closed 진입점)."""
    reports = evaluate(days)
    print_report(reports)
    blocked = [r.dt for r in reports if r.blocked]
    if blocked:
        raise RuntimeError(f"품질 게이트 FAIL — 파티션 {blocked}. 다운스트림 차단.")
    return reports


def main(days: list[str]) -> int:
    reports = evaluate(days)
    print_report(reports)
    return 1 if any(r.blocked for r in reports) else 0


if __name__ == "__main__":
    argv = sys.argv[1:] or ["2026-07-05", "2026-07-06", "2026-07-07"]
    raise SystemExit(main(argv))
