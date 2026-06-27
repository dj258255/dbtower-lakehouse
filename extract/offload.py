"""query_snapshot 일 배치 Extract & Load 핵심 로직.

설계 원칙(docs/ROADMAP.md Phase 1):
- 관측 전용 메타 PG에서만 읽는다. 읽기 전용 + 어제 시간창 + 서버커서 배치.
- 인덱스 idx_snapshot_instance_time(instance_id, captured_at)의 선두 컬럼을
  타도록 instance_id별로 루프 돈다(captured_at 단독 조건은 선두를 못 탄다).
- parquet 스키마를 명시 선언한다(조용한 타입 추론 변화 차단).
- 멱등: 파티션 프리픽스를 통째로 지우고 다시 쓴다. 같은 dt를 몇 번 돌려도 행수 불변.
  단, 원천이 0행인데 기존 파티션이 존재하면 삭제하지 않고 시끄럽게 실패한다
  (Phase 8 — 원천 보존 밖 dt 재실행에서 아카이브 유일본을 지우는 자기파괴 차단).

이 모듈은 Airflow와 독립적으로도 실행된다(로컬 e2e 검증용).
Airflow DAG(dags/snapshot_offload.py)는 run_offload를 얇게 감쌀 뿐이다.
"""
from __future__ import annotations

import io
import logging
from datetime import date, datetime, timedelta

import boto3
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from extract.config import (
    FETCH_BATCH_SIZE,
    RAW_PREFIX,
    SOURCE_TABLE,
    SinkConfig,
    SourceConfig,
)

log = logging.getLogger("snapshot_offload")

# 원천 스키마를 명시 선언한다 — 이후 타입 추론이 흔들려도 parquet 스키마는 고정된다.
# captured_at은 파티션 키(dt)로 빠지지 않고 값으로도 남긴다(구간 내 시각 보존 → Phase 2 델타 계산에 필요).
SNAPSHOT_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("instance_id", pa.int64(), nullable=False),
        pa.field("captured_at", pa.timestamp("us"), nullable=False),
        pa.field("query_id", pa.string(), nullable=False),
        pa.field("query_text", pa.string(), nullable=True),
        pa.field("calls", pa.int64(), nullable=False),
        pa.field("total_time_ms", pa.float64(), nullable=False),
        pa.field("rows_examined", pa.int64(), nullable=False),
    ]
)

_SELECT_COLUMNS = (
    "id, instance_id, captured_at, query_id, query_text, "
    "calls, total_time_ms, rows_examined"
)


def _s3_client(sink: SinkConfig):
    return boto3.client(
        "s3",
        endpoint_url=sink.endpoint,
        aws_access_key_id=sink.access_key,
        aws_secret_access_key=sink.secret_key,
        region_name=sink.region,
        config=BotoConfig(signature_version="s3v4"),
    )


def _ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        log.info("버킷 %s 없음 → 생성", bucket)
        s3.create_bucket(Bucket=bucket)


def _list_instance_ids(conn) -> list[int]:
    """어느 instance를 훑을지는 레지스트리(database_instance)에서 가져온다.

    query_snapshot을 captured_at만으로 DISTINCT 스캔하면 인덱스 선두를 못 타므로,
    등록된 인스턴스 목록을 먼저 얻어 그 각각에 대해 인덱스를 타는 등치 질의를 돈다.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM database_instance ORDER BY id")
        return [row[0] for row in cur.fetchall()]


def _fetch_partition(conn, instance_id: int, day_start: datetime, day_end: datetime):
    """한 instance의 하루치를 서버커서로 배치 읽어 pyarrow Table로 만든다.

    WHERE instance_id = %s AND captured_at >= %s AND captured_at < %s
    → idx_snapshot_instance_time(instance_id, captured_at)를 그대로 탄다.
    """
    ids, iids, caps, qids, qtexts, calls, times, rows = [], [], [], [], [], [], [], []
    # named cursor = 서버 사이드 커서. 결과 전체를 클라이언트 메모리에 올리지 않는다.
    with conn.cursor(name=f"offload_{instance_id}_{day_start:%Y%m%d}") as cur:
        cur.itersize = FETCH_BATCH_SIZE
        cur.execute(
            f"SELECT {_SELECT_COLUMNS} FROM {SOURCE_TABLE} "
            "WHERE instance_id = %s AND captured_at >= %s AND captured_at < %s "
            "ORDER BY captured_at",
            (instance_id, day_start, day_end),
        )
        for r in cur:
            ids.append(r[0])
            iids.append(r[1])
            caps.append(r[2])
            qids.append(r[3])
            qtexts.append(r[4])
            calls.append(r[5])
            times.append(r[6])
            rows.append(r[7])

    if not ids:
        return None

    return pa.Table.from_arrays(
        [
            pa.array(ids, type=pa.int64()),
            pa.array(iids, type=pa.int64()),
            pa.array(caps, type=pa.timestamp("us")),
            pa.array(qids, type=pa.string()),
            pa.array(qtexts, type=pa.string()),
            pa.array(calls, type=pa.int64()),
            pa.array(times, type=pa.float64()),
            pa.array(rows, type=pa.int64()),
        ],
        schema=SNAPSHOT_SCHEMA,
    )


def _delete_prefix(s3, bucket: str, prefix: str) -> int:
    """파티션 프리픽스 아래를 통째로 지운다(멱등 재적재의 핵심)."""
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
            deleted += len(objs)
    return deleted


def _prefix_exists(s3, bucket: str, prefix: str) -> bool:
    """파티션 프리픽스 아래에 오브젝트가 하나라도 있는가(존재 확인만, 삭제 없음)."""
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


class ArchiveSelfDestructError(RuntimeError):
    """원천이 0행인데 기존 파티션(유일본일 수 있음)을 덮어쓰려 한 시도.

    원천 보존(7일) 밖의 dt를 backfill/Clear로 재실행하면 원천은 이미 비어 있고,
    이때 delete-first 멱등 덮어쓰기는 '아카이브 유일본 삭제 후 아무것도 안 씀'이
    된다. 그 경로를 여기서 시끄럽게 끊는다 — 예외로 태스크를 죽여 재시도·webhook
    알림 경로에 태운다. 삭제는 절대 하지 않는다.
    """


def decide_partition_action(source_rows: int, partition_exists: bool) -> str:
    """파티션 처리 결정(순수 로직 — 테스트 대상).

    - 원천 N행                     → "overwrite"  (기존 delete→write 멱등 경로)
    - 원천 0행 + 파티션 없음       → "skip"       (정말 아무것도 없는 날)
    - 원천 0행 + 파티션 존재       → 예외          (유일본 파괴 차단, fail-closed)
    """
    if source_rows > 0:
        return "overwrite"
    if not partition_exists:
        return "skip"
    raise ArchiveSelfDestructError(
        "원천 0행인데 기존 파티션 오브젝트가 존재 — 보존 창 밖 재적재로 판단. "
        "이 파티션이 유일본일 수 있어 삭제를 거부한다(fail-closed). "
        "정말 지워야 하면 사람이 명시적으로 지운 뒤 재실행할 것."
    )


def parse_logical_date(logical_date: str | date) -> date:
    """'YYYY-MM-DD' 또는 date를 date로 정규화. 형식이 어긋나면 즉시 실패."""
    if isinstance(logical_date, str):
        return datetime.strptime(logical_date, "%Y-%m-%d").date()
    return logical_date


def day_window(dt: date) -> tuple[datetime, datetime]:
    """dt의 UTC 반열림 하루 창 [00:00, 다음날 00:00)."""
    day_start = datetime(dt.year, dt.month, dt.day)
    return day_start, day_start + timedelta(days=1)


def run_offload(logical_date: str | date) -> dict:
    """logical_date(어제)의 스냅샷을 instance별 parquet로 MinIO에 적재한다.

    반환: {"dt", "instances": {instance_id: rows}, "total_rows"} — Airflow XCom·검증용.
    """
    dt = parse_logical_date(logical_date)
    day_start, day_end = day_window(dt)

    src, sink = SourceConfig(), SinkConfig()
    s3 = _s3_client(sink)
    _ensure_bucket(s3, sink.bucket)

    result: dict = {"dt": dt.isoformat(), "instances": {}, "total_rows": 0}

    conn = psycopg2.connect(src.dsn())
    # 읽기 전용 트랜잭션 — 원천을 절대 바꾸지 않는다는 계약을 세션 레벨로 못박는다.
    conn.set_session(readonly=True, autocommit=False)
    try:
        instance_ids = _list_instance_ids(conn)
        log.info("dt=%s 대상 instance %s", dt, instance_ids)

        for instance_id in instance_ids:
            table = _fetch_partition(conn, instance_id, day_start, day_end)
            prefix = f"{RAW_PREFIX}/dt={dt.isoformat()}/instance_id={instance_id}/"

            # 아카이브 자기파괴 가드: 원천이 비었을 때는 삭제가 먼저 오면 안 된다.
            # 원천 보존 밖 dt의 재실행에서 기존 parquet가 유일본일 수 있기 때문.
            source_rows = table.num_rows if table is not None else 0
            action = decide_partition_action(
                source_rows, _prefix_exists(s3, sink.bucket, prefix)
            )
            if action == "skip":
                log.info("instance %s: 원천 0행·파티션 없음 → 스킵", instance_id)
                continue

            # 정상 흐름(원천 N행)만 여기 도달 — 기존 delete→write 멱등 경로 유지.
            removed = _delete_prefix(s3, sink.bucket, prefix)
            if removed:
                log.info("기존 파티션 오브젝트 %d개 삭제 (%s)", removed, prefix)

            buf = io.BytesIO()
            pq.write_table(table, buf, compression="zstd")
            buf.seek(0)
            key = prefix + "part-000.parquet"
            s3.put_object(Bucket=sink.bucket, Key=key, Body=buf.getvalue())

            n = table.num_rows
            result["instances"][instance_id] = n
            result["total_rows"] += n
            log.info("instance %s: %d행 → s3://%s/%s", instance_id, n, sink.bucket, key)
    finally:
        conn.rollback()  # 읽기 전용이라 커밋할 게 없다.
        conn.close()

    log.info("적재 완료 dt=%s 총 %d행", dt, result["total_rows"])
    return result


if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    day = sys.argv[1] if len(sys.argv) > 1 else (date.today() - timedelta(days=1)).isoformat()
    print(json.dumps(run_offload(day), ensure_ascii=False, indent=2))
