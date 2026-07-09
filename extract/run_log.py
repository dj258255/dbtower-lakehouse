"""파이프라인 런 메타 로그 — 운영 대시보드의 데이터 원천 (Phase 10).

알림(webhook)은 "실패하면 운다"이고 heartbeat는 "성공의 부재를 잡는다"였다. 둘 다
이벤트/역이벤트다 — "지금 파이프라인이 전반적으로 어떤 상태인가"를 한 화면으로 보는
수단은 없었다(감사 백로그: 알림과 화면의 이원화). 이 모듈은 매 런의 메타
(dt·행수·게이트 3축·소요시간·heartbeat)를 **DuckLake 테이블 pipeline_run_log**에
남긴다. 마트와 같은 카탈로그(=PG)·같은 데이터 경로(S3)라 Metabase가 이미 붙은
DuckLake 커넥션으로 그대로 읽는다 — 서비스·커넥션 추가 0.

분석 대시보드(악화 쿼리)와 운영 대시보드(게이트 상태·최근 런·마지막 성공 dt)를
이원화한다: 전자는 "데이터가 뭘 말하나", 후자는 "파이프라인이 건강한가".

append-only 로그다(런마다 INSERT). "최근 런 N개"·"마지막 성공 dt"·"오늘 게이트
상태"가 전부 이 한 테이블의 질의로 나온다.

    python -m extract.run_log 2026-07-05 2026-07-06   # 실게이트 돌려 실측 행 기록
"""
from __future__ import annotations

import logging
import sys
import time
from datetime import UTC, datetime

from extract.config import DuckLakeConfig, SinkConfig
from extract.ducklake_load import open_lake

log = logging.getLogger("run_log")

RUN_LOG_TABLE = "pipeline_run_log"

# DuckLake 테이블 스키마 — 카탈로그에 박힌다(추론 아님).
_CREATE = f"""
CREATE TABLE IF NOT EXISTS {RUN_LOG_TABLE} (
    dt                   DATE,
    run_id               VARCHAR,
    run_at               TIMESTAMP,
    duration_sec         DOUBLE,
    gate_status          VARCHAR,
    gate_reconciliation  VARCHAR,
    gate_completeness    VARCHAR,
    gate_freshness       VARCHAR,
    gate_schema_drift    VARCHAR,
    offload_rows         BIGINT,
    published_rows       BIGINT,
    heartbeat_at         TIMESTAMP
)
"""


def build_record(
    dt: str,
    gate_axes: dict[str, str],
    gate_status: str,
    duration_sec: float,
    run_id: str | None = None,
    offload_rows: int | None = None,
    published_rows: int | None = None,
    heartbeat_at: datetime | None = None,
    run_at: datetime | None = None,
) -> dict:
    """런 메타를 pipeline_run_log 행 dict로 조립한다(순수 함수 — 테스트 대상).

    gate_axes는 {'reconciliation','completeness','freshness','schema_drift': status}.
    빠진 축은 None으로 남는다(계약 밖 상태를 지어내지 않는다).
    """
    return {
        "dt": dt,
        "run_id": run_id,
        "run_at": run_at or datetime.now(UTC),
        "duration_sec": round(float(duration_sec), 3),
        "gate_status": gate_status,
        "gate_reconciliation": gate_axes.get("reconciliation"),
        "gate_completeness": gate_axes.get("completeness"),
        "gate_freshness": gate_axes.get("freshness"),
        "gate_schema_drift": gate_axes.get("schema_drift"),
        "offload_rows": offload_rows,
        "published_rows": published_rows,
        "heartbeat_at": heartbeat_at,
    }


_COLUMNS = (
    "dt", "run_id", "run_at", "duration_sec", "gate_status",
    "gate_reconciliation", "gate_completeness", "gate_freshness",
    "gate_schema_drift", "offload_rows", "published_rows", "heartbeat_at",
)


def append(
    record: dict,
    cfg: DuckLakeConfig | None = None,
    sink: SinkConfig | None = None,
) -> None:
    """런 메타 한 행을 DuckLake pipeline_run_log에 INSERT 한다(append-only)."""
    cfg = cfg or DuckLakeConfig()
    sink = sink or SinkConfig()
    con = open_lake(cfg, sink)
    try:
        con.execute(_CREATE)
        placeholders = ", ".join("?" for _ in _COLUMNS)
        con.execute(
            f"INSERT INTO {RUN_LOG_TABLE} ({', '.join(_COLUMNS)}) VALUES ({placeholders})",
            [record.get(c) for c in _COLUMNS],
        )
        log.info("run_log 기록 dt=%s gate=%s (%.1fs)",
                 record.get("dt"), record.get("gate_status"), record.get("duration_sec") or 0)
    finally:
        con.close()


def _axes_from_reports(reports) -> tuple[dict[str, str], str]:
    """quality.evaluate() 결과에서 축별 상태와 종합 상태를 뽑는다."""
    rep = reports[0]
    axes = {c.name: c.status for c in rep.checks}
    return axes, rep.status


def emit_for_dt(dt: str, cfg: DuckLakeConfig | None = None) -> dict:
    """dt 하나에 실제 품질 게이트를 돌려 시간을 재고 run_log 행을 기록한다(실측 경로).

    DAG의 성공 경로가 heartbeat에서 쓰는 것과 같은 build_record/append를 쓴다 —
    운영 대시보드 데이터를 파이프라인 밖에서도 진짜로 채운다.
    """
    from extract.quality import _duck, _parquet_counts, evaluate, print_report

    t0 = time.monotonic()
    reports = evaluate([dt])
    dur = time.monotonic() - t0
    print_report(reports)
    axes, status = _axes_from_reports(reports)
    # 적재 행수(운영 카드용) — 그 dt 파티션의 parquet 행수 합.
    con = _duck(SinkConfig())
    try:
        offload_rows = sum(_parquet_counts(con, SinkConfig(), dt).values()) or None
    finally:
        con.close()
    record = build_record(
        dt=dt, gate_axes=axes, gate_status=status, duration_sec=dur,
        run_id=f"cli-{datetime.now(UTC):%Y%m%dT%H%M%S}",
        offload_rows=offload_rows,
        heartbeat_at=datetime.now(UTC) if status != "FAIL" else None,
    )
    append(record, cfg=cfg)
    return record


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    days = sys.argv[1:] or ["2026-07-05", "2026-07-06"]
    for d in days:
        rec = emit_for_dt(d)
        print(f"[run_log] dt={rec['dt']} gate={rec['gate_status']} "
              f"dur={rec['duration_sec']}s → DuckLake.{RUN_LOG_TABLE}")
