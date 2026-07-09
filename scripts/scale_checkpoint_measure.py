"""365dt CHECKPOINT 실측 — 1년치 일일 발행이 쌓은 스냅샷을 컴팩션하는 데 걸리는 시간.

publish는 매일 마트를 DuckLake로 커밋한다 → 1년이면 365 스냅샷 + 누적 소파일.
방치하면 카탈로그·S3가 단조 증가한다(Phase 6 유지보수의 존재 이유). 규모에서
CHECKPOINT(만료+플러시+컴팩션)가 얼마나 걸리는지, 파일이 얼마나 줄어드는지 잰다.

**격리**: 실운영 카탈로그(ducklake_catalog)가 아니라 별도 DB(ducklake_scale) +
별도 데이터 경로(s3://lakehouse/scale/ducklake/)에만 쓴다 — 실데이터 마트·스냅샷
무손상. 끝나면 scale_synthesize cleanup + 이 스크립트의 drop이 정리한다.

    python -m scripts.scale_checkpoint_measure  DUCKDB_PATH
"""
from __future__ import annotations

import sys
import time
from dataclasses import replace

from extract.config import DuckLakeConfig, SinkConfig
from extract.ducklake_load import ensure_catalog_db, open_lake

SCALE_CATALOG_DB = "ducklake_scale"
SCALE_DATA_PATH = "s3://lakehouse/scale/ducklake/"


def _scale_cfg() -> DuckLakeConfig:
    return replace(DuckLakeConfig(), catalog_db=SCALE_CATALOG_DB, data_path=SCALE_DATA_PATH)


def measure(duckdb_path: str) -> None:
    cfg, sink = _scale_cfg(), SinkConfig()
    ensure_catalog_db(cfg)
    con = open_lake(cfg, sink)
    try:
        # 재실행 대비 초기화(격리 카탈로그라 안전).
        con.execute("DROP TABLE IF EXISTS fct_scale")
        con.execute(
            "CREATE TABLE fct_scale ("
            "instance_id BIGINT, query_id VARCHAR, dt DATE, delta_calls HUGEINT, "
            "delta_total_time_ms DOUBLE, avg_latency_ms DOUBLE)"
        )
        con.execute(f"ATTACH '{duckdb_path}' AS src (READ_ONLY)")
        dts = [r[0] for r in con.execute(
            "SELECT DISTINCT dt FROM src.fct_query_daily ORDER BY dt").fetchall()]
        print(f"[적재] {len(dts)} dt를 하루=한 커밋으로 발행(1년치 일일 publish 모사)...")
        t0 = time.monotonic()
        for i, dt in enumerate(dts):
            con.execute(
                "INSERT INTO fct_scale SELECT instance_id, query_id, dt, delta_calls, "
                "delta_total_time_ms, avg_latency_ms FROM src.fct_query_daily WHERE dt = ?",
                [dt],
            )
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{len(dts)} 커밋", flush=True)
        load_t = time.monotonic() - t0

        snaps_before = con.execute(
            f"SELECT count(*) FROM ducklake_snapshots('{cfg.lake_alias}')").fetchone()[0]
        files_before = con.execute(
            f"SELECT count(*) FROM ducklake_list_files('{cfg.lake_alias}', 'fct_scale')"
        ).fetchone()[0]
        rows = con.execute("SELECT count(*) FROM fct_scale").fetchone()[0]

        print(f"[적재 완료] {rows:,}행, {len(dts)} 커밋 {load_t:.1f}s "
              f"(스냅샷 {snaps_before}, 활성 파일 {files_before})")

        # CHECKPOINT — 만료(0s)+플러시+컴팩션 번들. 규모에서 이게 얼마나 걸리나.
        con.execute(f"CALL {cfg.lake_alias}.set_option('expire_older_than', '0 seconds')")
        t0 = time.monotonic()
        con.execute(f"CHECKPOINT {cfg.lake_alias}")
        removed = con.execute(
            f"CALL ducklake_cleanup_old_files('{cfg.lake_alias}', cleanup_all => true)"
        ).fetchall()
        ckpt_t = time.monotonic() - t0

        snaps_after = con.execute(
            f"SELECT count(*) FROM ducklake_snapshots('{cfg.lake_alias}')").fetchone()[0]
        files_after = con.execute(
            f"SELECT count(*) FROM ducklake_list_files('{cfg.lake_alias}', 'fct_scale')"
        ).fetchone()[0]
        rows_after = con.execute("SELECT count(*) FROM fct_scale").fetchone()[0]

        print("\n=== CHECKPOINT 실측 (365dt = 1년치 일일 발행) ===")
        print(f"{'지표':<20} {'전':>10} {'후':>10}")
        print("-" * 42)
        print(f"{'스냅샷 수':<20} {snaps_before:>10,} {snaps_after:>10,}")
        print(f"{'활성 데이터 파일':<20} {files_before:>10,} {files_after:>10,}")
        print(f"{'행수(불변식)':<20} {rows:>10,} {rows_after:>10,}")
        print(f"CHECKPOINT 소요: {ckpt_t:.2f}s  (삭제 파일 {len(removed)}개)")
        assert rows == rows_after, "CHECKPOINT가 행수를 바꿨다 — 불변식 위반"
    finally:
        con.close()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "dbt/dbtower_lakehouse/dbtower_lakehouse.duckdb"
    measure(path)
