"""검증: MinIO에 적재된 parquet 행수가 원천 메타 PG와 정확히 일치하는가.

추출과 동일한 조건(captured_at >= day_start AND < day_end)으로 PG를 세고,
DuckDB(httpfs)로 s3의 parquet를 세서 dt별로 대조한다. 조용히 틀린 데이터를
막는 품질 게이트의 최소형이다.

    python -m extract.verify_count 2026-07-05 2026-07-06
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta

import duckdb
import psycopg2

from extract.config import RAW_PREFIX, SinkConfig, SourceConfig


def pg_count(src: SourceConfig, dt: str) -> int:
    day_start = datetime.strptime(dt, "%Y-%m-%d")
    day_end = day_start + timedelta(days=1)
    with psycopg2.connect(src.dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM query_snapshot "
            "WHERE captured_at >= %s AND captured_at < %s",
            (day_start, day_end),
        )
        return cur.fetchone()[0]


def parquet_count(con: duckdb.DuckDBPyConnection, sink: SinkConfig, dt: str) -> int:
    glob = f"s3://{sink.bucket}/{RAW_PREFIX}/dt={dt}/**/*.parquet"
    return con.execute(
        f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning = true)"
    ).fetchone()[0]


def main(days: list[str]) -> int:
    src, sink = SourceConfig(), SinkConfig()
    con = duckdb.connect()
    host = sink.endpoint.replace("http://", "").replace("https://", "")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{host}';")
    con.execute(f"SET s3_access_key_id='{sink.access_key}';")
    con.execute(f"SET s3_secret_access_key='{sink.secret_key}';")
    con.execute("SET s3_use_ssl=false; SET s3_url_style='path';")

    print(f"{'dt':<12} {'source PG':>12} {'parquet(S3)':>14}  {'match':>6}")
    print("-" * 48)
    ok = True
    for dt in days:
        p, q = pg_count(src, dt), parquet_count(con, sink, dt)
        match = "OK" if p == q else "MISMATCH"
        ok = ok and (p == q)
        print(f"{dt:<12} {p:>12,} {q:>14,}  {match:>6}")
    print("-" * 48)
    print("RESULT:", "ALL MATCH" if ok else "MISMATCH DETECTED")
    return 0 if ok else 1


if __name__ == "__main__":
    argv = sys.argv[1:] or ["2026-07-05", "2026-07-06"]
    raise SystemExit(main(argv))
