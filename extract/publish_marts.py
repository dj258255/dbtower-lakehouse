"""dbt 마트를 DuckLake로 발행(publish) — 대시보드 서빙 계층 (Phase 7).

왜 필요한가: dbt-duckdb는 마트를 로컬 DuckDB "파일"에 짓는다. 파일은 빠르고 단순하지만
**프로세스 간 단일 쓰기**다. 같은 호스트에선 BI(Metabase)가 읽기 전용으로라도 물고
있으면 매일 새벽 transform(dbt run)이 배타 잠금을 못 잡아 죽고("Conflicting lock is
held" 실측), 컨테이너 경계(virtiofs)에선 그 잠금마저 전파되지 않아 dbt가 열린 리더
밑에서 파일을 소리 없이 재작성한다(실측 — docs/VERIFICATION.md 8절). 시끄럽게 죽거나
조용히 위험하거나 — 어느 쪽이든 대시보드가 붙는 순간 파일은 서빙 계층으로 실격이다.

해법: transform이 끝나면 마트 테이블을 **DuckLake**(카탈로그=PG, 데이터=S3)로 복사한다.
DuckLake는 카탈로그가 PG 트랜잭션이라 읽는 쪽(대시보드)과 쓰는 쪽(이 태스크)이 서로를
막지 않고, 발행마다 스냅샷이 쌓여 "그 날 대시보드가 뭘 보여줬나"도 타임트래블로 남는다.
raw(query_snapshot)가 이미 사는 곳이라 서비스 추가도 0이다.

마트는 일간 집계라 작다(수천 행) — 통째 CREATE OR REPLACE가 증분보다 단순하고 멱등하다.
DROP+CREATE가 DuckLake에선 하나의 커밋(스냅샷)이라, 읽는 쪽은 언제나 발행 전이나 후의
온전한 버전만 본다(반쪽 테이블 없음). 단 그 보장은 테이블 하나 단위였다 — Phase 8부터
마트 전체를 단일 트랜잭션(BEGIN…COMMIT)으로 묶어, 중간 실패 시 "새 fct + 어제 mart"
혼합 버전이 대시보드에 노출되는 경로까지 차단한다(전부 나가거나 전부 안 나가거나).

    python -m extract.publish_marts          # 호스트에서 수동 발행
"""
from __future__ import annotations

import logging
import os

from extract.config import DuckLakeConfig, SinkConfig
from extract.ducklake_load import open_lake

log = logging.getLogger("publish_marts")

# 발행 대상 마트(= dbt marts 디렉터리의 테이블 materialization 전부).
# Phase 14: fct_query_hourly(D5)·mart_baseline_longterm(D6) 편입 — 후자는 되쓰기(D7) 화물.
MART_TABLES = (
    "fct_query_daily",
    "mart_query_regression",
    "fct_query_hourly",
    "mart_baseline_longterm",
    "fct_size_daily",
    "mart_capacity_forecast",
)

# dbt가 마트를 짓는 DuckDB 파일. 컨테이너에선 compose가 절대경로로 덮어쓴다.
DEFAULT_DUCKDB_PATH = os.getenv(
    "DBT_DUCKDB_PATH", "dbt/dbtower_lakehouse/dbtower_lakehouse.duckdb"
)


def publish_marts(
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    cfg: DuckLakeConfig | None = None,
    sink: SinkConfig | None = None,
) -> dict[str, int]:
    """dbt DuckDB 파일의 마트를 DuckLake로 통째 발행하고 {테이블: 행수}를 반환한다.

    - 파일은 READ_ONLY로 연다(발행이 원본을 건드릴 수 없게).
    - 테이블마다 발행 후 행수를 원본과 대조한다 — 다르면 예외(불변식 위반).
    """
    cfg = cfg or DuckLakeConfig()
    sink = sink or SinkConfig()

    con = open_lake(cfg, sink)
    try:
        con.execute(f"ATTACH '{duckdb_path}' AS marts_src (READ_ONLY)")
        published: dict[str, int] = {}
        # 마트 전부를 DuckLake 단일 트랜잭션(=단일 스냅샷)으로 발행한다.
        # 개별 커밋이면 fct 성공·mart 실패 시 대시보드가 "새 fct + 어제 mart"라는
        # 존재한 적 없는 혼합 버전을 보게 된다(Phase 8 주입 실측). 마트끼리는
        # 같은 dt의 산출물 — 함께 나가거나 함께 안 나가야 한다.
        con.execute("BEGIN")
        try:
            for table in MART_TABLES:
                src_count = con.execute(
                    f"SELECT count(*) FROM marts_src.{table}"
                ).fetchone()[0]
                con.execute(
                    f"CREATE OR REPLACE TABLE {cfg.lake_alias}.{table} AS "
                    f"SELECT * FROM marts_src.{table}"
                )
                lake_count = con.execute(
                    f"SELECT count(*) FROM {cfg.lake_alias}.{table}"
                ).fetchone()[0]
                if lake_count != src_count:
                    raise RuntimeError(
                        f"발행 불변식 위반: {table} 원본 {src_count}행 != lake {lake_count}행"
                    )
                published[table] = lake_count
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        for table, rows in published.items():
            log.info("발행 %s → %s.%s (%s행)", table, cfg.lake_alias, table, rows)
        return published
    finally:
        con.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    result = publish_marts()
    for table, rows in result.items():
        print(f"[발행] {table}: {rows:,}행 → DuckLake")
