"""파이프라인 heartbeat — "성공했다"를 주기적으로 남기는 생존 신호 (Phase 9).

기존 알림(extract/alerts.py)은 "실패하면 운다"다. 그런데 스케줄러가 통째로 죽거나
DAG가 pause되거나 원천 수집기가 조용히 멈추면 태스크 자체가 시작을 안 하므로
on_failure_callback이 불릴 일도 없다 — 아무도 울지 않는다(감사 지적: 원천 수집기가
21시간 침묵했는데 알림 0). 이 구멍은 "실패를 감지"하는 방향으론 못 막는다. "성공이
주기적으로 남겨야 할 신호가 안 남았다"는 역방향(deadman)으로만 잡힌다.

이 모듈은 그 신호의 절반 — 성공 시 heartbeat를 남기는 쪽 — 이다. 나머지 절반(신호가
끊기면 경보)은 extract/deadman.py다.

저장소: DuckLake 카탈로그와 같은 PG 인스턴스의 분리 DB(ducklake_catalog)에
pipeline_heartbeat 테이블. 서비스 추가 0이고, DBTower 메타 DB(dbtower)는 오염시키지
않는다(Phase 5부터 지켜온 분리). 파일이 아니라 테이블이라 컨테이너가 죽어도 남고,
SQL로 조회·감시된다.

    python -m extract.heartbeat snapshot_offload      # 수동 heartbeat 기록
"""
from __future__ import annotations

import logging
import sys
from datetime import UTC, datetime

import psycopg2

from extract.config import DuckLakeConfig
from extract.ducklake_load import ensure_catalog_db

log = logging.getLogger("heartbeat")

HEARTBEAT_TABLE = "pipeline_heartbeat"

_CREATE = f"""
CREATE TABLE IF NOT EXISTS {HEARTBEAT_TABLE} (
    dag_id          TEXT PRIMARY KEY,
    last_success_at TIMESTAMPTZ NOT NULL,
    run_id          TEXT,
    note            TEXT
)
"""

_UPSERT = f"""
INSERT INTO {HEARTBEAT_TABLE} (dag_id, last_success_at, run_id, note)
VALUES (%s, %s, %s, %s)
ON CONFLICT (dag_id) DO UPDATE
   SET last_success_at = EXCLUDED.last_success_at,
       run_id          = EXCLUDED.run_id,
       note            = EXCLUDED.note
"""


def _connect(cfg: DuckLakeConfig):
    """카탈로그 DB에 접속. 없으면 만든다(Phase 5 데모 전에도 heartbeat가 서게)."""
    ensure_catalog_db(cfg)  # 멱등 — 이미 있으면 no-op.
    return psycopg2.connect(cfg.catalog_dsn())


def ensure_table(cfg: DuckLakeConfig | None = None) -> None:
    cfg = cfg or DuckLakeConfig()
    with _connect(cfg) as conn, conn.cursor() as cur:
        cur.execute(_CREATE)
        conn.commit()


def write_heartbeat(
    dag_id: str,
    run_id: str | None = None,
    note: str | None = None,
    at: datetime | None = None,
    cfg: DuckLakeConfig | None = None,
) -> datetime:
    """dag_id의 성공 heartbeat를 기록(upsert)하고 기록 시각을 반환한다."""
    cfg = cfg or DuckLakeConfig()
    ts = at or datetime.now(UTC)
    with _connect(cfg) as conn, conn.cursor() as cur:
        cur.execute(_CREATE)
        cur.execute(_UPSERT, (dag_id, ts, run_id, note))
        conn.commit()
    log.info("heartbeat 기록 %s @ %s (run_id=%s)", dag_id, ts.isoformat(), run_id)
    return ts


def read_heartbeats(cfg: DuckLakeConfig | None = None) -> dict[str, datetime]:
    """{dag_id: last_success_at} — deadman 감시가 읽는다. 테이블 없으면 빈 dict."""
    cfg = cfg or DuckLakeConfig()
    with _connect(cfg) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
            (HEARTBEAT_TABLE,),
        )
        if not cur.fetchone():
            return {}
        cur.execute(f"SELECT dag_id, last_success_at FROM {HEARTBEAT_TABLE}")
        return {r[0]: r[1] for r in cur.fetchall()}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    dag_id = sys.argv[1] if len(sys.argv) > 1 else "manual"
    ts = write_heartbeat(dag_id, run_id="manual", note="extract.heartbeat 수동 기록")
    print(f"[heartbeat] {dag_id} ← {ts.isoformat()}")
