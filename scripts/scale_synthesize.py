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

**N축 모드(Phase 12)** — 10단계가 시간축(dt)만 늘렸다면, 이 모드는 **인스턴스축(N)**을
늘린다. 설계 핵심은 *총량 고정, 축만 회전*: 10단계가 365dt×6inst=2,190파일·54.5M행이었으니
N축은 7dt×300inst=2,100파일·~52M행으로 맞춘다 — 총량이 같으면 측정 차이가 전부
"축의 모양"(per-dt 파일 수 6→300)으로 귀속된다. 리매핑은 닫힌 dt의 실제 인스턴스를
순환 복제하되 instance_id를 1..N으로 갈아끼우고, query_id에 `~i{j}` suffix를 붙여
고유쿼리 카디널리티도 N에 비례해 자연 증가시킨다(id는 j*1e9 오프셋으로 충돌 방지).
격리 프리픽스는 `scale_n/`(dt축의 `scale/`과도 분리). 악화 주입은 하지 않는다 —
N축 측정 대상은 랭킹이 아니라 파이프라인 역학이다.

    python -m scripts.scale_synthesize generate --instances 300 --days 7
    python -m scripts.scale_synthesize cleanup   # scale/ 와 scale_n/ 둘 다 정리
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
# N축(인스턴스 수) 합성 프리픽스 — dt축(scale/)과도 분리(Phase 12).
SCALE_N_PREFIX = "scale_n/query_snapshot"
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


def _remap_instance(t: pa.Table, new_iid: int) -> pa.Table:
    """base 인스턴스 테이블을 합성 인스턴스 new_iid의 것으로 리매핑한 사본.

    - instance_id → new_iid (상수 컬럼 교체)
    - query_id    → 원본 + '~i{new_iid}' suffix — 인스턴스마다 자기 쿼리 모집단을
      갖는 실제 배포처럼, 고유쿼리 카디널리티가 N에 비례해 자연 증가한다.
    - id          → 원본 + new_iid*1e9 오프셋(전역 유일 유지)
    """
    n = t.num_rows
    ids = pa.array([v + new_iid * 1_000_000_000 for v in t.column("id").to_pylist()],
                   type=pa.int64())
    iids = pa.array([new_iid] * n, type=pa.int64())
    qids = pa.array([f"{q}~i{new_iid}" for q in t.column("query_id").to_pylist()],
                    type=pa.string())
    out = t.set_column(t.schema.get_field_index("id"), "id", ids)
    out = out.set_column(out.schema.get_field_index("instance_id"), "instance_id", iids)
    return out.set_column(out.schema.get_field_index("query_id"), "query_id", qids)


def generate_n(instances: int, days: int) -> None:
    """N축 합성(Phase 12) — 총량 고정·축 회전. scale_n/ 프리픽스에만 쓴다."""
    sink = SinkConfig()
    s3 = _s3(sink)
    print(f"[N축 생성] 원본 dt={SOURCE_DT} 읽는 중(원천 PG 무접속)...")
    base = _duck_read_source(sink)
    base_ids = sorted(base)
    base_rows = sum(t.num_rows for t in base.values())
    print(f"[N축 생성] base 인스턴스 {base_ids}(행 {base_rows:,}) → 합성 N={instances}")

    # 합성 인스턴스별 parquet 바이트를 미리 직렬화 — base를 순환 복제 + 리매핑.
    synth_bytes: dict[int, bytes] = {}
    per_dt_rows = 0
    for j in range(1, instances + 1):
        src = base[base_ids[(j - 1) % len(base_ids)]]
        remapped = _remap_instance(src, j)
        synth_bytes[j] = _serialize(remapped)
        per_dt_rows += remapped.num_rows
        if j % 100 == 0 or j == instances:
            print(f"[N축 생성] 직렬화 {j}/{instances}", flush=True)
    per_dt_bytes = sum(len(b) for b in synth_bytes.values())

    dts = [END_DT - timedelta(days=days - 1 - k) for k in range(days)]
    put = 0
    for k, dt in enumerate(dts):
        for j in range(1, instances + 1):
            key = f"{SCALE_N_PREFIX}/dt={dt.isoformat()}/instance_id={j}/part-000.parquet"
            s3.put_object(Bucket=sink.bucket, Key=key, Body=synth_bytes[j])
            put += 1
        print(f"[N축 생성] dt {k + 1}/{days} (누적 {put} 오브젝트)", flush=True)

    print(f"[완료] {days}dt × {instances}인스턴스 = {put} 오브젝트, "
          f"총 {per_dt_rows * days:,}행 · 약 {per_dt_bytes * days / 1e6:.1f}MB")
    print(f"[격리] 실데이터 raw/·dt축 scale/ 무손상. 프리픽스 = "
          f"s3://{sink.bucket}/{SCALE_N_PREFIX}/")


def cleanup() -> None:
    sink = SinkConfig()
    s3 = _s3(sink)
    paginator = s3.get_paginator("list_objects_v2")
    for label, prefix in (("scale/", SCALE_PREFIX), ("scale_n/", SCALE_N_PREFIX)):
        removed = 0
        for page in paginator.paginate(Bucket=sink.bucket, Prefix=f"{prefix}/"):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                s3.delete_objects(Bucket=sink.bucket, Delete={"Objects": objs})
                removed += len(objs)
        print(f"[정리] {label} 프리픽스 오브젝트 {removed}개 삭제")
    print("[정리] 실데이터 raw/ 는 불변")


def main() -> int:
    ap = argparse.ArgumentParser(description="365dt 규모 실측용 합성 데이터")
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--days", type=int, default=365)
    g.add_argument("--instances", type=int, default=None,
                   help="N축 모드(Phase 12): 인스턴스 N개로 리매핑 복제 → scale_n/")
    sub.add_parser("cleanup")
    args = ap.parse_args()
    if args.cmd == "generate":
        if args.instances:
            generate_n(args.instances, args.days)
        else:
            generate(args.days)
    elif args.cmd == "cleanup":
        cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
