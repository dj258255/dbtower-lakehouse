"""365dt 규모 실측용 합성 데이터 생성·정리 (Phase 10).

며칠치(실데이터 3dt)로는 "규모에서도 버틴다"를 증명하지 못한다. 1년치(365dt)를
만들어 재보고, 수치가 요구할 때만 최적화한다 — 그 1년치를 여기서 만든다.

**격리 원칙**: 실데이터(raw/)를 절대 건드리지 않는다. 합성 파티션은 별도 프리픽스
`scale/query_snapshot/` 아래에만 쓴다(같은 버킷, 다른 경로). 원천 PG도 읽지 않는다
(닫힌 dt=2026-07-05의 기존 parquet를 날짜 시프트 복제할 뿐). 실측이 끝나면
`cleanup`으로 프리픽스를 통째 지운다 — 실데이터·마트는 원상.

**생성 방식(서버 부하 0, 원천 오염 0)**:
- 닫힌 dt=2026-07-05의 6개 인스턴스 parquet(149,259행)를 메모리로 읽는다.
- 그것을 365개 날짜 경로(dt=...)에 복제한다. 파일 크기·개수 프로파일이 실제와
  동일하다(인스턴스당 1파일 = dt당 6파일, zstd) → 작은 파일 폭증을 그대로 재현.
- 롤링 윈도우 검증을 위해 **마지막 7일**의 total_time_ms에만 쿼리별 악화 계수를
  곱한다(hash 버킷: 60% 안정 / +50% / +150%). 최근 7일 vs 직전 30일 비교에서
  악화 쿼리가 랭킹으로 떠야 하므로, 그 신호를 결정적으로 심는다.

    python -m scripts.scale_synthesize generate --days 365
    python -m scripts.scale_synthesize cleanup
"""
from __future__ import annotations

import argparse
import io
from datetime import date, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config as BotoConfig

from extract.config import RAW_PREFIX, SinkConfig

# 합성 데이터 격리 프리픽스 — 실데이터 raw/ 와 물리적으로 분리.
SCALE_PREFIX = "scale/query_snapshot"
# 복제 원본 = 닫힌 dt(불변 149,259행). 원천 PG를 다시 읽지 않는다.
SOURCE_DT = "2026-07-05"
# 롤링 윈도우 신호를 심을 "최근" 구간 길이(일).
RECENT_DEGRADE_DAYS = 7
# 마지막 날짜(생성 구간의 오른쪽 끝). recent 윈도우 = END_DT - 6 .. END_DT.
END_DT = date(2026, 7, 6)

# 쿼리별 악화 계수 — hash(query_id) % 5 버킷. 안정(0,1,2) / +50%(3) / +150%(4).
DEGRADE_BY_BUCKET = {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.5, 4: 1.5}


def _s3(sink: SinkConfig):
    return boto3.client(
        "s3",
        endpoint_url=sink.endpoint,
        aws_access_key_id=sink.access_key,
        aws_secret_access_key=sink.secret_key,
        region_name=sink.region,
        config=BotoConfig(signature_version="s3v4", connect_timeout=5, read_timeout=120),
    )


def _duck_read_source(sink: SinkConfig) -> dict[int, pa.Table]:
    """SOURCE_DT의 인스턴스별 parquet를 pyarrow Table로 읽는다(원천 PG 무접속)."""
    import duckdb

    con = duckdb.connect()
    host = sink.endpoint.replace("http://", "").replace("https://", "")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{host}';")
    con.execute(f"SET s3_access_key_id='{sink.access_key}';")
    con.execute(f"SET s3_secret_access_key='{sink.secret_key}';")
    con.execute("SET s3_use_ssl=false; SET s3_url_style='path';")
    tables: dict[int, pa.Table] = {}
    glob = f"s3://{sink.bucket}/{RAW_PREFIX}/dt={SOURCE_DT}/instance_id=*/*.parquet"
    ids = [r[0] for r in con.execute(
        f"SELECT DISTINCT instance_id FROM read_parquet('{glob}', hive_partitioning=1) ORDER BY 1"
    ).fetchall()]
    for iid in ids:
        one = f"s3://{sink.bucket}/{RAW_PREFIX}/dt={SOURCE_DT}/instance_id={iid}/*.parquet"
        # dt(파티션 컬럼)는 빼고 원본 8컬럼만 — 경로가 dt를 준다(offload 계약과 동일).
        tables[iid] = con.execute(
            "SELECT id, instance_id, captured_at, query_id, query_text, "
            f"calls, total_time_ms, rows_examined FROM read_parquet('{one}')"
        ).to_arrow_table()
    con.close()
    return tables


def _degrade_factor(query_id: str) -> float:
    return 1.0 + DEGRADE_BY_BUCKET[hash(query_id) % 5]


def _boost_table(t: pa.Table) -> pa.Table:
    """최근 구간용 — total_time_ms를 쿼리별 악화 계수로 스케일한 사본.

    한 파티션 전체에 쿼리별 균일 계수를 곱하므로 하루 양 끝(first/last) 차분도 같은
    계수로 스케일된다 → avg_latency(delta_time/delta_calls)가 그 계수만큼 오른다.
    """
    qids = t.column("query_id").to_pylist()
    times = t.column("total_time_ms").to_pylist()
    new_times = [tm * _degrade_factor(q) for q, tm in zip(qids, times, strict=True)]
    idx = t.schema.get_field_index("total_time_ms")
    return t.set_column(idx, "total_time_ms", pa.array(new_times, type=pa.float64()))


def _serialize(t: pa.Table) -> bytes:
    buf = io.BytesIO()
    pq.write_table(t, buf, compression="zstd")
    return buf.getvalue()


def generate(days: int) -> None:
    sink = SinkConfig()
    s3 = _s3(sink)
    print(f"[생성] 원본 dt={SOURCE_DT} 읽는 중(원천 PG 무접속, 기존 parquet 복제)...")
    base = _duck_read_source(sink)
    instances = sorted(base)
    print(f"[생성] 인스턴스 {instances}, 원본 행수 {sum(t.num_rows for t in base.values()):,}")

    # 인스턴스별로 normal/boosted 두 변형의 parquet 바이트를 미리 직렬화(반복 압축 회피).
    normal_bytes = {i: _serialize(base[i]) for i in instances}
    boosted_bytes = {i: _serialize(_boost_table(base[i])) for i in instances}
    per_dt_bytes = sum(len(b) for b in normal_bytes.values())

    dts = [END_DT - timedelta(days=days - 1 - k) for k in range(days)]
    recent_cut = END_DT - timedelta(days=RECENT_DEGRADE_DAYS - 1)  # 이 날짜부터 boosted
    put = 0
    for k, dt in enumerate(dts):
        variant = boosted_bytes if dt >= recent_cut else normal_bytes
        for iid in instances:
            key = f"{SCALE_PREFIX}/dt={dt.isoformat()}/instance_id={iid}/part-000.parquet"
            s3.put_object(Bucket=sink.bucket, Key=key, Body=variant[iid])
            put += 1
        if (k + 1) % 50 == 0 or k == len(dts) - 1:
            print(f"[생성] {k + 1}/{days} dt (누적 {put} 오브젝트)", flush=True)

    print(f"[완료] {days}dt × {len(instances)}인스턴스 = {put} 오브젝트")
    print(f"[완료] dt당 {per_dt_bytes:,}B → 총 약 {per_dt_bytes * days / 1e6:.1f}MB")
    print(f"[완료] recent(악화 주입) 구간: {recent_cut} .. {END_DT} "
          f"({RECENT_DEGRADE_DAYS}일), 그 앞은 안정 baseline")
    print(f"[격리] 실데이터 raw/ 는 무손상. 프리픽스 = s3://{sink.bucket}/{SCALE_PREFIX}/")


def cleanup() -> None:
    sink = SinkConfig()
    s3 = _s3(sink)
    paginator = s3.get_paginator("list_objects_v2")
    removed = 0
    for page in paginator.paginate(Bucket=sink.bucket, Prefix=f"{SCALE_PREFIX}/"):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=sink.bucket, Delete={"Objects": objs})
            removed += len(objs)
    print(f"[정리] scale/ 프리픽스 오브젝트 {removed}개 삭제 — 실데이터 raw/ 는 불변")


def main() -> int:
    ap = argparse.ArgumentParser(description="365dt 규모 실측용 합성 데이터")
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--days", type=int, default=365)
    sub.add_parser("cleanup")
    args = ap.parse_args()
    if args.cmd == "generate":
        generate(args.days)
    elif args.cmd == "cleanup":
        cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
