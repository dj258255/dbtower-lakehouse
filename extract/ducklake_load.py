"""DuckLake 테이블 포맷 — lake를 house로 (Phase 5).

지금까지(Phase 1~4) raw는 파티션 parquet를 통째로 덮어쓰는 방식이었다. 정확하고
멱등하지만, 엄밀히는 아직 "lake"다 — 덮어쓰기엔 ACID도, 과거를 되돌아볼 타임트래블도
없다. 어제 상태가 무엇이었는지는 파일이 이미 사라져 알 수 없다.

이 모듈은 그 위에 **테이블 포맷 DuckLake**를 얹는다.
  - 카탈로그(어떤 스냅샷에 어떤 파일이 속하는지 = 메타데이터)는 **PostgreSQL**에 둔다.
    로컬에 이미 PG가 있어 서비스 추가가 0이다. 단 DBTower 메타 DB(dbtower)를 오염시키지
    않으려고 **별도 DB `ducklake_catalog`**를 쓴다.
  - 데이터 파일(실제 컬럼나 parquet)은 **MinIO(S3)**에 둔다. 스토리지/컴퓨트 분리.

그러면 모든 변경이 스냅샷(버전)으로 쌓이고, `AT (VERSION => n)`으로 과거를 그대로
다시 질의할 수 있다. ACID·타임트래블·스키마 진화 = lake가 house가 되는 지점.

    python -m extract.ducklake_load          # 전체 실증(적재→버전→타임트래블→롤백)

주의: 수치는 **닫힌 UTC 창**(dt=2026-07-05·07-06)만 쓴다. 07-07은 원천 DB의 시계
기준 아직 진행 중인 '오늘'이라 값이 자라므로 재현 수치로 쓰지 않는다.
"""
from __future__ import annotations

import logging
import os
import sys
from urllib.parse import urlparse

import duckdb
import psycopg2

from extract.config import RAW_PREFIX, DuckLakeConfig, SinkConfig

log = logging.getLogger("ducklake_load")

# DuckLake 테이블 스키마 — raw 계약(CONTRACT.md)을 그대로 따르되, 파티션 키였던 dt를
# 값 컬럼으로 명시 선언한다. 테이블 포맷이므로 스키마는 카탈로그에 박힌다(추론 아님).
TABLE_NAME = "query_snapshot"
_CREATE_TABLE = f"""
CREATE TABLE {TABLE_NAME} (
    id            BIGINT,
    instance_id   BIGINT,
    captured_at   TIMESTAMP,
    query_id      VARCHAR,
    query_text    VARCHAR,
    calls         BIGINT,
    total_time_ms DOUBLE,
    rows_examined BIGINT,
    dt            DATE
)
"""


def _s3_endpoint_hostport(sink: SinkConfig) -> tuple[str, bool]:
    """'http://localhost:19000' → ('localhost:19000', use_ssl=False)."""
    parsed = urlparse(sink.endpoint)
    hostport = parsed.netloc or parsed.path
    return hostport, parsed.scheme == "https"


def ensure_catalog_db(cfg: DuckLakeConfig) -> bool:
    """카탈로그 전용 PG DB가 없으면 만든다(멱등). DBTower 메타 DB는 건드리지 않는다.

    반환: 이번 호출에서 새로 만들었으면 True.
    """
    conn = psycopg2.connect(cfg.admin_dsn())
    conn.autocommit = True  # CREATE DATABASE는 트랜잭션 안에서 못 돈다.
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (cfg.catalog_db,))
            if cur.fetchone():
                return False
            cur.execute(f'CREATE DATABASE "{cfg.catalog_db}"')
            log.info("카탈로그 DB %s 생성", cfg.catalog_db)
            return True
    finally:
        conn.close()


def open_lake(cfg: DuckLakeConfig, sink: SinkConfig) -> duckdb.DuckDBPyConnection:
    """DuckDB를 열고 ducklake 카탈로그(PG)+데이터(S3)를 ATTACH해 USE까지 잡는다."""
    con = duckdb.connect()
    for ext in ("ducklake", "postgres", "httpfs"):
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")

    hostport, use_ssl = _s3_endpoint_hostport(sink)
    con.execute(
        """
        CREATE OR REPLACE SECRET minio (
            TYPE s3, KEY_ID ?, SECRET ?, ENDPOINT ?,
            URL_STYLE 'path', USE_SSL ?, REGION ?
        )
        """,
        [sink.access_key, sink.secret_key, hostport, use_ssl, sink.region],
    )
    con.execute(
        f"ATTACH 'ducklake:postgres:{cfg.catalog_dsn()}' AS {cfg.lake_alias} "
        f"(DATA_PATH '{cfg.data_path}')"
    )
    con.execute(f"USE {cfg.lake_alias}")
    return con


def _raw_glob(dt: str) -> str:
    return f"s3://{SinkConfig().bucket}/{RAW_PREFIX}/dt={dt}/instance_id=*/*.parquet"


def load_partition(con: duckdb.DuckDBPyConnection, dt: str) -> int:
    """닫힌 dt의 raw parquet를 DuckLake 테이블에 INSERT하고 적재 행수를 반환한다.

    이 INSERT 하나가 하나의 커밋(스냅샷)이 된다.
    """
    before = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    con.execute(
        f"""
        INSERT INTO {TABLE_NAME}
        SELECT id, instance_id, captured_at, query_id, query_text,
               calls, total_time_ms, rows_examined, CAST(dt AS DATE)
        FROM read_parquet('{_raw_glob(dt)}', hive_partitioning = 1)
        """
    )
    after = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    return after - before


def current_version(con: duckdb.DuckDBPyConnection, cfg: DuckLakeConfig) -> int:
    """가장 최근 커밋된 스냅샷 버전 id."""
    return con.execute(
        f"SELECT max(snapshot_id) FROM ducklake_snapshots('{cfg.lake_alias}')"
    ).fetchone()[0]


def snapshots(con: duckdb.DuckDBPyConnection, cfg: DuckLakeConfig) -> list[tuple]:
    """(버전, 시각, 변경요약) 스냅샷 목록."""
    return con.execute(
        f"""
        SELECT snapshot_id, snapshot_time, changes
        FROM ducklake_snapshots('{cfg.lake_alias}')
        ORDER BY snapshot_id
        """
    ).fetchall()


# 재현 대상 = 닫힌 UTC 창만. 07-07(진행 중인 오늘)은 쓰지 않는다.
CLOSED_LATER = "2026-07-06"   # 79,894행 (불변)
CLOSED_EARLIER = "2026-07-05"  # 149,259행 (불변)


def _table_exists(con, cfg: DuckLakeConfig, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = 'main' AND table_name = ?",
        [cfg.lake_alias, table],
    ).fetchone()
    return row is not None


def _confirm_destructive_drop(table: str, row_count: int, force: bool) -> bool:
    """기존 테이블을 DROP하기 전 확인 관문 — 데모가 운영 데이터를 말없이 지우면 안 된다.

    허용 경로 셋: force 인자(코드 호출), DUCKLAKE_DEMO_FORCE=1(비대화형),
    TTY 대화형 y 입력. 그 외에는 전부 거부한다(fail-closed).
    """
    if force or os.getenv("DUCKLAKE_DEMO_FORCE") == "1":
        return True
    if sys.stdin.isatty():
        ans = input(
            f"[경고] DuckLake 테이블 {table}({row_count:,}행)이 이미 존재한다. "
            f"데모는 이 테이블을 DROP 후 재생성한다. 계속? [y/N] "
        )
        return ans.strip().lower() == "y"
    return False


def run_demo(
    cfg: DuckLakeConfig | None = None,
    sink: SinkConfig | None = None,
    force: bool = False,
) -> None:
    """ACID·타임트래블 전 과정을 실제로 돌려 증거 출력을 낸다.

    커밋을 네 번 쌓는다: CREATE → 07-06 적재 → 07-05 적재 → 한 행 UPDATE.
    그다음 타임트래블로 과거 버전이 현재와 다름을 실제 조회하고, 트랜잭션 롤백으로
    원자성을 보인다. 버전 번호는 카탈로그에서 동적으로 읽어 재실행에도 견딘다.

    파괴성 주의: 재실행 대비 초기화로 기존 query_snapshot을 DROP한다. 기존
    테이블이 있으면 확인(force / DUCKLAKE_DEMO_FORCE=1 / 대화형 y) 없이는 안 지운다.
    """
    cfg = cfg or DuckLakeConfig()
    sink = sink or SinkConfig()

    created = ensure_catalog_db(cfg)
    print(f"[카탈로그 DB] {cfg.catalog_db} @ {cfg.catalog_host}:{cfg.catalog_port} "
          f"({'신규 생성' if created else '기존 재사용'}) — DBTower 메타 DB(dbtower)와 분리")

    con = open_lake(cfg, sink)
    print(f"[ATTACH] ducklake:postgres → DATA_PATH {cfg.data_path}  (카탈로그=PG, 데이터=S3)")

    # 재실행 대비 초기화. 단 DROP은 파괴적이므로 기존 테이블이 있으면 확인을 요구한다.
    if _table_exists(con, cfg, TABLE_NAME):
        n = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
        if not _confirm_destructive_drop(TABLE_NAME, n, force):
            con.close()
            raise SystemExit(
                f"중단: 기존 {TABLE_NAME}({n:,}행) 보존. 재생성하려면 --force 또는 "
                f"DUCKLAKE_DEMO_FORCE=1로 명시할 것."
            )
        con.execute(f"DROP TABLE {TABLE_NAME}")

    con.execute(_CREATE_TABLE)
    v_create = current_version(con, cfg)
    print(f"\n[커밋1] CREATE TABLE {TABLE_NAME}  → 버전 {v_create}")

    n06 = load_partition(con, CLOSED_LATER)
    v_load06 = current_version(con, cfg)
    c06 = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"[커밋2] INSERT dt={CLOSED_LATER}  +{n06:,}행  → 버전 {v_load06}  (누적 {c06:,})")

    n05 = load_partition(con, CLOSED_EARLIER)
    v_load05 = current_version(con, cfg)
    c05 = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"[커밋3] INSERT dt={CLOSED_EARLIER}  +{n05:,}행  → 버전 {v_load05}  (누적 {c05:,})")

    # 한 행 UPDATE — '뒤늦게 정정된 스냅샷' 시뮬레이션. 타임트래블로 되돌아볼 대상.
    target_id, old_val = con.execute(
        f"SELECT id, total_time_ms FROM {TABLE_NAME} "
        f"WHERE dt = DATE '{CLOSED_LATER}' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    new_val = old_val + 1000.0
    con.execute(
        f"UPDATE {TABLE_NAME} SET total_time_ms = ? WHERE id = ?", [new_val, target_id]
    )
    v_update = current_version(con, cfg)
    print(f"[커밋4] UPDATE id={target_id} total_time_ms {old_val:.2f} → {new_val:.2f}  "
          f"→ 버전 {v_update}  (행수 불변 {c05:,})")

    print("\n=== 스냅샷(버전) 목록 — ducklake_snapshots ===")
    for sid, stime, changes in snapshots(con, cfg):
        print(f"  v{sid}  {stime:%Y-%m-%d %H:%M:%S}  {changes}")

    print("\n=== 타임트래블 — 같은 테이블, 버전별 상이한 결과 ===")
    tt_q = f"SELECT count(*) FROM {TABLE_NAME} AT (VERSION => {{v}})"
    for label, v in [(f"v{v_load06} (07-06만 적재 직후)", v_load06),
                     (f"v{v_load05} (07-05까지 적재 직후)", v_load05),
                     (f"v{v_update} (현재)", v_update)]:
        cnt = con.execute(tt_q.format(v=v)).fetchone()[0]
        print(f"  count @ {label:32s} = {cnt:,}")

    print(f"\n=== 타임트래블 — 한 행의 값이 버전 사이에서 다름 (id={target_id}) ===")
    val_before = con.execute(
        f"SELECT total_time_ms FROM {TABLE_NAME} AT (VERSION => {v_load05}) WHERE id = {target_id}"
    ).fetchone()[0]
    val_now = con.execute(
        f"SELECT total_time_ms FROM {TABLE_NAME} WHERE id = {target_id}"
    ).fetchone()[0]
    print(f"  total_time_ms @ v{v_load05}(과거) = {val_before:.2f}")
    print(f"  total_time_ms @ v{v_update}(현재) = {val_now:.2f}")
    print("  → 과거 버전이 UPDATE 이전 값을 그대로 보존(달라짐 확인)")

    print("\n=== 원자성 — BEGIN … ROLLBACK ===")
    snaps_before = len(snapshots(con, cfg))
    cnt_before = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    con.execute("BEGIN")
    con.execute(f"DELETE FROM {TABLE_NAME} WHERE dt = DATE '{CLOSED_EARLIER}'")
    cnt_in_txn = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    con.execute("ROLLBACK")
    cnt_after = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    snaps_after = len(snapshots(con, cfg))
    print(f"  트랜잭션 전 count       = {cnt_before:,}")
    print(f"  DELETE 07-05 후(txn 내) = {cnt_in_txn:,}")
    print(f"  ROLLBACK 후 count       = {cnt_after:,}  (원상복구)")
    print(f"  스냅샷 수 {snaps_before} → {snaps_after}  (롤백은 버전을 남기지 않음)")

    con.close()
    print("\n완료 — lake가 house가 되었다(ACID·타임트래블·버전 카탈로그).")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="DuckLake ACID·타임트래블 데모")
    parser.add_argument(
        "--force", action="store_true",
        help="기존 query_snapshot이 있어도 확인 없이 DROP 후 재생성(파괴적)",
    )
    args = parser.parse_args()
    run_demo(force=args.force)
