"""데이터 품질 게이트 — 다운스트림(dbt) 앞에 세우는 fail-closed 검문소.

조용히 틀린 데이터는 없는 것보다 나쁘다. raw가 반쪽만 적재됐는데 그 위에 마트를
만들면 "악화 쿼리 랭킹"이 조용히 오답을 낸다. 이 모듈은 dt 파티션이 다운스트림에
넘어가기 전에 네 가지를 검문한다.

  1) reconciliation(정합)  — 원천 PG 행수 == parquet 행수. 인스턴스별로 대조.
                             하나라도 어긋나면 FAIL(적재 유실·중복·부분 적재 탐지).
  2) completeness(완결성)  — 레지스트리(database_instance)의 기대 인스턴스가
                             그 dt 파티션에 전부 존재하는가. 빠지면 FAIL(수집 누락).
  3) freshness(신선도)     — 그 dt의 최신 captured_at이 하루 경계(다음날 00:00)에
                             충분히 근접한가. 임계 초과 시 WARN, 심하면 FAIL
                             (수집이 하루 중간에 끊긴 반쪽 파티션 탐지).
  4) schema drift(스키마)  — 원천 information_schema를 offload의 SNAPSHOT_SCHEMA
                             기대와 대조. 컬럼 유실·타입 불일치 FAIL(추출이 깨질
                             변화), 원천에 컬럼이 늘어난 것은 WARN(추출은 되지만
                             새 컬럼이 버려지고 있다는 신호).

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
from extract.tables import PRIMARY_TABLE, REGISTRY, TableSpec

# freshness 임계: dt의 최신 captured_at이 다음날 00:00에서 이만큼 이상 벌어지면 경보.
# WARN = 수집이 늦게 끊겼을 수 있음(경고, 차단 안 함). FAIL = 하루 절반 이상 비었음(차단).
FRESHNESS_WARN_HOURS = float(os.getenv("QUALITY_FRESHNESS_WARN_HOURS", "3"))
FRESHNESS_FAIL_HOURS = float(os.getenv("QUALITY_FRESHNESS_FAIL_HOURS", "12"))

# SKIP = 이 테이블의 게이트 프로필(Phase 14 D4)이 끈 축 — "안 쟀음"을 보고서에 남긴다.
# 검사 생략을 조용히 지우면 "다 통과"로 오독된다(정직 표기).
OK, WARN, FAIL, SKIP = "OK", "WARN", "FAIL", "SKIP"


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


def _pg_counts(
    src: SourceConfig, dt: str, instances: list[int], spec: TableSpec | None = None
) -> dict[int, int]:
    """원천 PG의 dt 하루창(UTC 반열림) 인스턴스별 행수.

    워터마크 단독 필터는 (instance_id, 시각) 복합 인덱스의 선두를 못 타 원천 전체
    Seq Scan이 된다(실측 332ms/31k버퍼 vs 인덱스 20ms/76버퍼). offload가 지킨 것과
    같은 원칙 — 레지스트리 인스턴스별로 등치 조건을 걸어 인덱스 선두를 태운다.
    Phase 14: 테이블·워터마크 컬럼을 스펙에서 일반화.
    """
    spec = spec or REGISTRY[PRIMARY_TABLE]
    day_start = datetime.strptime(dt, "%Y-%m-%d")
    day_end = day_start + timedelta(days=1)
    counts: dict[int, int] = {}
    with psycopg2.connect(src.dsn()) as conn, conn.cursor() as cur:
        for iid in instances:
            cur.execute(
                f"SELECT count(*) FROM {spec.name} "
                f"WHERE instance_id = %s AND {spec.watermark_col} >= %s "
                f"AND {spec.watermark_col} < %s",
                (iid, day_start, day_end),
            )
            n = int(cur.fetchone()[0])
            if n:
                counts[iid] = n
    return counts


def _parquet_counts(
    con: duckdb.DuckDBPyConnection, sink: SinkConfig, dt: str, prefix: str = RAW_PREFIX
) -> dict[int, int]:
    """적재된 parquet의 dt 파티션 인스턴스별 행수. 파티션이 통째로 없으면 빈 dict."""
    glob = f"s3://{sink.bucket}/{prefix}/dt={dt}/instance_id=*/*.parquet"
    try:
        rows = con.execute(
            f"SELECT instance_id, count(*) FROM read_parquet('{glob}', hive_partitioning = 1) "
            "GROUP BY instance_id"
        ).fetchall()
    except duckdb.IOException:
        # glob이 아무 파일과도 안 맞으면 IOException — 파티션 전무로 간주.
        return {}
    return {int(r[0]): int(r[1]) for r in rows}


def _parquet_max_captured(
    con: duckdb.DuckDBPyConnection, sink: SinkConfig, dt: str,
    prefix: str = RAW_PREFIX, watermark_col: str = "captured_at",
):
    glob = f"s3://{sink.bucket}/{prefix}/dt={dt}/instance_id=*/*.parquet"
    try:
        row = con.execute(
            f"SELECT max({watermark_col}) FROM read_parquet('{glob}', hive_partitioning = 1)"
        ).fetchone()
    except duckdb.IOException:
        return None
    return row[0] if row else None


# 원천 계약(docs/CONTRACT.md) — offload의 SNAPSHOT_SCHEMA가 기대하는 PG 컬럼·타입.
# 원천 스키마가 여기서 벗어나면 추출이 깨지거나(유실·타입 변경 = FAIL)
# 데이터가 조용히 버려진다(원천에만 있는 새 컬럼 = WARN).
EXPECTED_SOURCE_COLUMNS: dict[str, str] = {
    "id": "bigint",
    "instance_id": "bigint",
    "captured_at": "timestamp without time zone",
    "query_id": "character varying",
    "query_text": "character varying",
    "calls": "bigint",
    "total_time_ms": "double precision",
    "rows_examined": "bigint",
}


def _source_columns(src: SourceConfig, table_name: str = "query_snapshot") -> dict[str, str]:
    """원천 테이블의 실제 {컬럼: data_type} — information_schema 기준."""
    with psycopg2.connect(src.dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = %s",
            (table_name,),
        )
        return {r[0]: r[1] for r in cur.fetchall()}


def check_schema_drift(
    actual: dict[str, str],
    expected: dict[str, str] | None = None,
) -> CheckResult:
    """원천 스키마 vs 추출 계약 대조(순수 로직 — 테스트 대상).

    - 기대 컬럼이 원천에서 사라짐 / 타입이 바뀜 → FAIL (다음 추출이 깨진다)
    - 원천에 기대 밖 컬럼이 생김               → WARN (추출은 돌지만 그 컬럼은 버려짐)
    """
    expected = expected if expected is not None else EXPECTED_SOURCE_COLUMNS
    missing = [c for c in expected if c not in actual]
    mismatched = [
        f"{c}: 기대 {expected[c]} != 실제 {actual[c]}"
        for c in expected
        if c in actual and actual[c] != expected[c]
    ]
    extra = [c for c in actual if c not in expected]
    if missing or mismatched:
        parts = []
        if missing:
            parts.append(f"컬럼 유실 {missing}")
        if mismatched:
            parts.append("타입 불일치 " + "; ".join(mismatched))
        return CheckResult("schema_drift", FAIL, " / ".join(parts))
    if extra:
        return CheckResult(
            "schema_drift", WARN,
            f"원천에 계약 밖 컬럼 {extra} — 추출은 돌지만 이 컬럼은 버려지는 중",
        )
    return CheckResult("schema_drift", OK, f"기대 {len(expected)}컬럼 전부 타입 일치")


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


def evaluate(days: list[str], table: str = PRIMARY_TABLE) -> list[DtReport]:
    """게이트 4축을 테이블 프로필(Phase 14 D4)대로 평가한다.

    프로필이 끈 축은 SKIP으로 기록 — backup_run처럼 저빈도·사후 변이 테이블에
    completeness/freshness를 재면 정상 상태가 fail-closed 오탐이 되기 때문(끄는
    이유는 extract/tables.py의 스펙 주석이 단일 진실).
    """
    spec = REGISTRY[table]
    src, sink = SourceConfig(), SinkConfig()
    registry = _registry_instances(src)
    con = _duck(sink)
    # 스키마 드리프트는 dt와 무관한 '지금 원천' 검사 — 한 번만 재고 모든 dt에 싣는다.
    schema_check = (
        check_schema_drift(_source_columns(src, spec.name), spec.expected_pg_columns)
        if spec.gate.schema_drift
        else CheckResult("schema_drift", SKIP, "프로필이 끔")
    )
    reports: list[DtReport] = []
    for dt in days:
        pq = _parquet_counts(con, sink, dt, prefix=spec.raw_prefix)
        rep = DtReport(dt=dt)
        if spec.gate.reconciliation:
            pg = _pg_counts(src, dt, registry, spec=spec)
            rep.checks.append(check_reconciliation(pg, pq))
        else:
            rep.checks.append(CheckResult("reconciliation", SKIP, "프로필이 끔"))
        if spec.gate.completeness:
            rep.checks.append(check_completeness(registry, pq))
        else:
            rep.checks.append(CheckResult(
                "completeness", SKIP, "저빈도 테이블 — 전 인스턴스 존재를 요구하면 오탐"))
        if spec.gate.freshness:
            mx = _parquet_max_captured(
                con, sink, dt, prefix=spec.raw_prefix, watermark_col=spec.watermark_col)
            rep.checks.append(check_freshness(mx, dt))
        else:
            rep.checks.append(CheckResult(
                "freshness", SKIP, "이벤트성 테이블 — 경계 근접 전제가 부적합"))
        rep.checks.append(schema_check)
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


def assert_gate(days: list[str], table: str = PRIMARY_TABLE) -> list[DtReport]:
    """게이트를 돌리고, FAIL이 있으면 예외를 던진다(Airflow/스크립트 fail-closed 진입점)."""
    reports = evaluate(days, table=table)
    print_report(reports)
    blocked = [r.dt for r in reports if r.blocked]
    if blocked:
        raise RuntimeError(f"품질 게이트 FAIL — {table} 파티션 {blocked}. 다운스트림 차단.")
    return reports


def main(days: list[str], table: str = PRIMARY_TABLE) -> int:
    reports = evaluate(days, table=table)
    print_report(reports)
    return 1 if any(r.blocked for r in reports) else 0


if __name__ == "__main__":
    argv = sys.argv[1:] or ["2026-07-05", "2026-07-06", "2026-07-07"]
    # 사용: python -m extract.quality DT [DT ...] [--table backup_run]
    tbl = PRIMARY_TABLE
    if "--table" in argv:
        i = argv.index("--table")
        tbl = argv[i + 1]
        argv = argv[:i] + argv[i + 2:]
    raise SystemExit(main(argv, table=tbl))
