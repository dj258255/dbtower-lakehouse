"""원천 테이블 레지스트리 — offload·게이트가 공유하는 단일 진실 (Phase 14, D3·D4).

Phase 13까지 offload는 `query_snapshot` 하나만 알았다(단수 상수 SOURCE_TABLE).
DBTower가 7일 뒤 버리는 데이터는 세 테이블 더 있다 — backup_run·plan_snapshot,
그리고 아직 원천에 영속 자체가 없는 wait_event(D1, DBTower 몫). 테이블마다
워터마크 컬럼·불변성·게이트 축이 달라서, 단수 상수를 복수로 늘리는 게 아니라
**테이블 스펙 레지스트리**로 일반화한다.

게이트 프로필(D4)이 여기 같이 사는 이유: 게이트 4축을 모든 테이블에 똑같이 재면
정상 상태가 오탐이 된다 — backup_run은 저빈도(백업 안 도는 인스턴스가 정상)라
completeness를 재면 매일 FAIL이고, 사후 변이(verify/remote가 나중에 UPDATE)라
freshness의 전제도 안 맞는다. 어떤 축을 강제할지는 테이블의 성질이므로 스펙의 일부다.
"""
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class GateProfile:
    """게이트 4축 중 이 테이블에 강제할 축 (D4).

    끈 축은 '검사 생략'이지 '항상 통과'가 아니다 — 보고서에 SKIP으로 남겨
    무엇을 안 쟀는지가 보이게 한다(정직 표기).
    """

    reconciliation: bool = True
    completeness: bool = True
    freshness: bool = True
    schema_drift: bool = True


@dataclass(frozen=True)
class TableSpec:
    """원천 테이블 하나의 추출 계약 (docs/CONTRACT.md §1과 짝)."""

    name: str                        # 원천 PG 테이블명
    select_columns: tuple[str, ...]  # 추출 컬럼(순서 = parquet 스키마 순서)
    schema: pa.Schema                # parquet 명시 스키마(타입 추론 변화 차단)
    watermark_col: str               # 하루창(dt) 필터 컬럼
    raw_prefix: str                  # raw/<이름> — 파티션 규약은 dt=/instance_id=/ 공통
    gate: GateProfile
    expected_pg_columns: dict[str, str]  # information_schema 기대(드리프트 검사)
    # False = 사후 변이 테이블(닫힌 dt가 불변이 아님 — D+1 스냅샷 계약, CONTRACT 참조)
    immutable: bool = True
    # False = 원천에 아직 테이블이 없음(D1 선결 대기). offload가 명확히 거부한다.
    available: bool = True


_QUERY_SNAPSHOT = TableSpec(
    name="query_snapshot",
    select_columns=(
        "id", "instance_id", "captured_at", "query_id", "query_text",
        "calls", "total_time_ms", "rows_examined",
    ),
    schema=pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("instance_id", pa.int64(), nullable=False),
        pa.field("captured_at", pa.timestamp("us"), nullable=False),
        pa.field("query_id", pa.string(), nullable=False),
        pa.field("query_text", pa.string(), nullable=True),
        pa.field("calls", pa.int64(), nullable=False),
        pa.field("total_time_ms", pa.float64(), nullable=False),
        pa.field("rows_examined", pa.int64(), nullable=False),
    ]),
    watermark_col="captured_at",
    raw_prefix="raw/query_snapshot",
    gate=GateProfile(),  # 4축 전부 — 고빈도·불변·전 인스턴스 수집이라 원 계약 그대로
    expected_pg_columns={
        "id": "bigint",
        "instance_id": "bigint",
        "captured_at": "timestamp without time zone",
        "query_id": "character varying",
        "query_text": "character varying",
        "calls": "bigint",
        "total_time_ms": "double precision",
        "rows_examined": "bigint",
    },
)

# 백업 이력 — 저빈도 + **사후 변이**(verify_status·verified_at·remote_location을
# 백업 후 별도 시점에 UPDATE). "닫힌 dt 불변" 전제가 깨지므로 D+1 스냅샷 계약:
# 어제 dt를 오늘 뽑아도 이후 verify가 갱신될 수 있음을 CONTRACT에 명시하고,
# 워터마크는 불변인 started_at을 쓴다.
_BACKUP_RUN = TableSpec(
    name="backup_run",
    select_columns=(
        "id", "instance_id", "started_at", "status", "backup_type", "duration_ms",
        "detail", "location", "verify_status", "verified_at", "remote_location",
    ),
    schema=pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("instance_id", pa.int64(), nullable=False),
        pa.field("started_at", pa.timestamp("us"), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("backup_type", pa.string(), nullable=True),
        pa.field("duration_ms", pa.int64(), nullable=False),
        pa.field("detail", pa.string(), nullable=True),
        pa.field("location", pa.string(), nullable=True),
        pa.field("verify_status", pa.string(), nullable=True),
        pa.field("verified_at", pa.timestamp("us"), nullable=True),
        pa.field("remote_location", pa.string(), nullable=True),
    ]),
    watermark_col="started_at",
    raw_prefix="raw/backup_run",
    # 저빈도(모든 인스턴스가 매일 백업하지 않음 = 정상) → completeness 오탐.
    # 사후 변이 + 이벤트성 → freshness("경계 근접") 전제 부적합. 정합·드리프트만.
    gate=GateProfile(completeness=False, freshness=False),
    expected_pg_columns={
        "id": "bigint",
        "instance_id": "bigint",
        "started_at": "timestamp without time zone",
        "status": "character varying",
        "backup_type": "character varying",
        "duration_ms": "bigint",
        "detail": "character varying",
        "location": "character varying",
        "verify_status": "character varying",
        "verified_at": "timestamp without time zone",
        "remote_location": "character varying",
    },
    immutable=False,
)

# 플랜 변경 이력 — 행 자체는 불변이나 **보존이 카운트 기반**(쿼리당 최신 20개 스윕,
# D2 선결)이라 하루가 닫히기 전 행이 지워질 수 있다. D2(DBTower: 시간 기반 보존
# 병행) 전까지는 "당일 추출분이 완전하지 않을 수 있음"을 CONTRACT에 정직 표기.
# 이벤트성(플랜 플립은 매일 안 생김) → completeness·freshness 오탐 → 정합·드리프트만.
_PLAN_SNAPSHOT = TableSpec(
    name="plan_snapshot",
    select_columns=(
        "id", "instance_id", "query_id", "plan_hash", "plan_shape", "captured_at",
    ),
    schema=pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("instance_id", pa.int64(), nullable=False),
        pa.field("query_id", pa.string(), nullable=False),
        pa.field("plan_hash", pa.string(), nullable=False),
        pa.field("plan_shape", pa.string(), nullable=True),
        pa.field("captured_at", pa.timestamp("us"), nullable=False),
    ]),
    watermark_col="captured_at",
    raw_prefix="raw/plan_snapshot",
    gate=GateProfile(completeness=False, freshness=False),
    expected_pg_columns={
        "id": "bigint",
        "instance_id": "bigint",
        "query_id": "character varying",
        "plan_hash": "character varying",
        "plan_shape": "character varying",
        "captured_at": "timestamp without time zone",
    },
)

# 대기 이벤트 — D1(DBTower V25) 완료로 편입(2026-07-18). 예약 시절의 단일 value 대신
# 실형은 wait_count·total_ms 두 측정값(조회 모델 WaitEvent record 그대로).
# 기종별 의미 차이는 원천의 정직 표기를 계승한다: MySQL/MSSQL/Oracle은 누적,
# PG는 현재 스냅샷, Mongo는 대기 큐 — 소비(마트)가 기종별로 해석해야 한다(CONTRACT §1-1).
# 미지원/무대기 기종은 그 사이클에 행이 없다 → completeness를 끄는 이유.
_WAIT_EVENT_SNAPSHOT = TableSpec(
    name="wait_event_snapshot",
    select_columns=(
        "id", "instance_id", "captured_at", "event_name", "category", "wait_count", "total_ms",
    ),
    schema=pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("instance_id", pa.int64(), nullable=False),
        pa.field("captured_at", pa.timestamp("us"), nullable=False),
        pa.field("event_name", pa.string(), nullable=False),
        pa.field("category", pa.string(), nullable=True),
        pa.field("wait_count", pa.int64(), nullable=False),
        pa.field("total_ms", pa.float64(), nullable=False),
    ]),
    watermark_col="captured_at",
    raw_prefix="raw/wait_event_snapshot",
    gate=GateProfile(completeness=False),
    expected_pg_columns={
        "id": "bigint",
        "instance_id": "bigint",
        "captured_at": "timestamp without time zone",
        "event_name": "character varying",
        "category": "character varying",
        "wait_count": "bigint",
        "total_ms": "double precision",
    },
)

# 오브젝트 크기 — 용량 예측(13단계)의 원료(DBTower V26, 2026-07-18 편입). 6시간 주기라
# freshness의 "경계 근접" 전제(연속 수집)가 안 맞아 끈다 — WARN 소음 방지, 사유는 SKIP으로 남음.
# volume_*·max_bytes는 임계 원천 ②(기종이 아는 볼륨) — 현 수집기는 NULL(후속 아크, 계약 nullable).
_SIZE_SNAPSHOT = TableSpec(
    name="size_snapshot",
    select_columns=(
        "id", "instance_id", "captured_at", "object_type", "object_name",
        "row_estimate", "data_bytes", "index_bytes",
        "volume_total_bytes", "volume_available_bytes", "max_bytes",
    ),
    schema=pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("instance_id", pa.int64(), nullable=False),
        pa.field("captured_at", pa.timestamp("us"), nullable=False),
        pa.field("object_type", pa.string(), nullable=False),
        pa.field("object_name", pa.string(), nullable=False),
        pa.field("row_estimate", pa.int64(), nullable=False),
        pa.field("data_bytes", pa.int64(), nullable=False),
        pa.field("index_bytes", pa.int64(), nullable=False),
        pa.field("volume_total_bytes", pa.int64(), nullable=True),
        pa.field("volume_available_bytes", pa.int64(), nullable=True),
        pa.field("max_bytes", pa.int64(), nullable=True),
    ]),
    watermark_col="captured_at",
    raw_prefix="raw/size_snapshot",
    gate=GateProfile(completeness=False, freshness=False),
    expected_pg_columns={
        "id": "bigint",
        "instance_id": "bigint",
        "captured_at": "timestamp without time zone",
        "object_type": "character varying",
        "object_name": "character varying",
        "row_estimate": "bigint",
        "data_bytes": "bigint",
        "index_bytes": "bigint",
        "volume_total_bytes": "bigint",
        "volume_available_bytes": "bigint",
        "max_bytes": "bigint",
    },
)

REGISTRY: dict[str, TableSpec] = {
    s.name: s
    for s in (_QUERY_SNAPSHOT, _BACKUP_RUN, _PLAN_SNAPSHOT, _WAIT_EVENT_SNAPSHOT, _SIZE_SNAPSHOT)
}

# 주 파이프라인(게이트 4축·dbt·publish·heartbeat)이 도는 테이블.
PRIMARY_TABLE = "query_snapshot"

# 보조 추출 대상 — available이고 primary가 아닌 것(D3의 aux offload 루프가 순회).
AUX_TABLES: tuple[str, ...] = tuple(
    name for name, s in REGISTRY.items() if s.available and name != PRIMARY_TABLE
)
