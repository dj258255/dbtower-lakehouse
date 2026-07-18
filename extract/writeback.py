"""장기 베이스라인 되쓰기(writeback) — 분석계가 운영계에 주는 유일한 것 (Phase 14 D7).

방향이 다른 일이다: forward(offload)는 원천을 **읽어** 내리고, 이 모듈은 원천 쪽
별도 테이블(baseline_longterm)에 **쓴다**. "되쓰기는 기계가 소비해 액션을 구동할
때만 정당" 원칙의 두 번째 사례 — DBTower BaselineService(D8)가 이 테이블을 읽어
7일 창 이상감지의 주간 계절성 오탐을 줄인다. 사람이 보는 용도가 아니다.

안전 설계(원천 readonly 봉인 유지 — 이 프로젝트의 안전 논거를 깨지 않는 핵심):
- **SourceConfig 재사용 금지.** 원천 접속(SRC_PG_*)은 계약·코드 양쪽에서 읽기
  전용이다(offload가 세션 readonly까지 건다). 되쓰기는 별도 역할 lakehouse_writer
  (해당 테이블만 INSERT/DELETE)를 별도 환경변수(WRITEBACK_PG_*)로 받는다.
- WRITEBACK_PG_HOST 미설정이면 no-op 스킵 — 되쓰기는 선택 기능이고, 켜지 않은
  배포에서 조용히 실패하는 대신 명시적으로 "안 함"을 로그로 남긴다.
- DELETE+INSERT를 **단일 트랜잭션**으로 — DBTower 이상감지 폴러가 도중에 읽어도
  PG MVCC가 이전 스냅샷을 보여준다(빈 테이블 순간 없음). publish의 원자성·행수
  대조 불변식을 그대로 이식했다.

테이블 DDL은 DBTower 쪽 Flyway(D8) 소유 — 여기서는 존재를 전제하고, 없으면
명확한 에러로 죽는다(조용히 CREATE하지 않는다: 스키마 소유권은 소비자에게).

    python -m extract.writeback            # WRITEBACK_PG_* 설정 시 되쓰기 실행
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import psycopg2

from extract.config import DuckLakeConfig, SinkConfig
from extract.ducklake_load import open_lake

log = logging.getLogger("writeback")

# 되쓰기 대상 — DuckLake에 발행된 마트(원본은 dbt가 계산).
BASELINE_MART = "mart_baseline_longterm"
# 원천 쪽 수신 테이블(DDL은 DBTower Flyway 소유 — D8).
TARGET_TABLE = "baseline_longterm"

_INSERT_COLUMNS = (
    "instance_id", "query_id", "dow", "hour",
    "mean_delta_calls", "stddev_delta_calls", "observations", "computed_at",
)


@dataclass(frozen=True)
class WritebackConfig:
    """되쓰기 전용 접속 — SourceConfig와 의도적으로 무관(readonly 봉인 유지)."""

    host: str = os.getenv("WRITEBACK_PG_HOST", "")
    port: int = int(os.getenv("WRITEBACK_PG_PORT", os.getenv("SRC_PG_PORT", "15432")))
    dbname: str = os.getenv("WRITEBACK_PG_DB", "dbtower")
    user: str = os.getenv("WRITEBACK_PG_USER", "lakehouse_writer")
    password: str = os.getenv("WRITEBACK_PG_PASSWORD", "")

    @property
    def enabled(self) -> bool:
        return bool(self.host and self.password)

    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password} connect_timeout=5"
        )


def run_writeback(
    cfg: WritebackConfig | None = None,
    lake_cfg: DuckLakeConfig | None = None,
    sink: SinkConfig | None = None,
) -> dict:
    """DuckLake의 장기 베이스라인 마트를 원천 쪽 baseline_longterm으로 되쓴다.

    반환: {"enabled", "rows"} — 미설정이면 {"enabled": False, "rows": 0} (no-op).
    """
    cfg = cfg or WritebackConfig()
    if not cfg.enabled:
        log.info("되쓰기 미설정(WRITEBACK_PG_HOST/PASSWORD 없음) — 스킵(no-op)")
        return {"enabled": False, "rows": 0}

    lake_cfg = lake_cfg or DuckLakeConfig()
    sink = sink or SinkConfig()

    # 1) 화물 적재 — DuckLake에 발행된 마트를 읽는다(read-only 소비).
    lake = open_lake(lake_cfg, sink)
    try:
        rows = lake.execute(
            f"SELECT {', '.join(_INSERT_COLUMNS)} "
            f"FROM {lake_cfg.lake_alias}.{BASELINE_MART}"
        ).fetchall()
    finally:
        lake.close()

    # 2) 단일 트랜잭션 DELETE+INSERT — 도중에 읽는 폴러는 MVCC로 이전 버전을 본다.
    conn = psycopg2.connect(cfg.dsn())
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TARGET_TABLE}")
            if rows:
                placeholders = ", ".join(["%s"] * len(_INSERT_COLUMNS))
                cur.executemany(
                    f"INSERT INTO {TARGET_TABLE} ({', '.join(_INSERT_COLUMNS)}) "
                    f"VALUES ({placeholders})",
                    rows,
                )
            cur.execute(f"SELECT count(*) FROM {TARGET_TABLE}")
            written = int(cur.fetchone()[0])
        if written != len(rows):
            conn.rollback()  # 행수 대조 불변식 — publish와 같은 규약.
            raise RuntimeError(
                f"되쓰기 불변식 위반: 마트 {len(rows)}행 != 쓰인 {written}행 — 롤백"
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()

    log.info("되쓰기 완료 — %s → %s (%d행, 단일 트랜잭션)", BASELINE_MART, TARGET_TABLE, written)
    return {"enabled": True, "rows": written}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    print(run_writeback())
