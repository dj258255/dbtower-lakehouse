"""CI용 tiny raw parquet 픽스처 생성 — 외부 DW 없이 dbt build를 e2e로 돌리기 위한 것.

이 스택의 강점은 쿼리 엔진(DuckDB)이 임베디드라는 점이다. MinIO도 PG도 없는 CI
러너에서, 원천 스냅샷을 흉내낸 작은 parquet 몇 장만 로컬에 깔면 dbt가 staging→fct
→mart를 실제로 짓고, 데이터 테스트·계약·unit test까지 전부 검증한다.

파티션 레이아웃은 운영과 동일하다:
    <out>/dt=YYYY-MM-DD/instance_id=N/part-000.parquet

CI는 RAW_SNAPSHOT_LOCATION 환경변수를 이 픽스처의 read_parquet(...)로 덮어써서
sources.yml의 기본값(MinIO s3://)을 대체한다(운영 경로는 그대로 유지).

    python scripts/ci_fixture.py /tmp/lh_fixture                 # parquet만
    python scripts/ci_fixture.py /tmp/lh_fixture /tmp/lh_ci.duckdb  # + raw 소스 뷰 등록

두 번째 인자(duckdb 파일)를 주면 raw.query_snapshot을 그 파일에 뷰로 등록한다.
dbt unit test는 입력 relation의 컬럼을 introspect하는데, 외부 read_parquet 소스는
물리 relation이 없어 introspect가 안 된다(dbt-duckdb 제약). 픽스처를 가리키는 뷰를
미리 만들어 두면 그 제약을 우회하면서도 여전히 외부 DW·MinIO·PG는 필요 없다.

데이터는 델타 로직의 대표 경로를 일부러 담는다:
  - 정상 누적 증가(하루 델타가 양수) — 마트에 악화 쿼리로 뜬다.
  - 지문 충돌(같은 instance/query/captured_at 두 행) — staging SUM 대상.
  - 순리셋(하루 last < first) — fct의 GREATEST(0,..) 클램프 대상.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# (instance_id, dt, query_id, query_text, [(captured_at, calls, total_time_ms, rows_examined), ...])
# calls/total_time_ms는 누적 카운터다(원천 계약). 하루 델타는 fct가 양 끝 차분으로 만든다.
_ROWS: list[tuple] = [
    # inst 1 · q1 — 이틀에 걸쳐 avg latency 10ms → 30ms 로 악화(마트 랭킹 대상).
    (1, "2026-01-01", "q1", "SELECT 1", [
        ("2026-01-01 06:00:00", 0, 0.0, 0),
        # 지문 충돌: 같은 (inst,query,captured_at)에 두 계열 — staging이 SUM으로 접는다.
        ("2026-01-01 12:00:00", 120, 1200.0, 1200),
        ("2026-01-01 12:00:00", 80, 800.0, 800),
        ("2026-01-01 23:00:00", 200, 2000.0, 2000),
    ]),
    (1, "2026-01-02", "q1", "SELECT 1", [
        ("2026-01-02 06:00:00", 1000, 20000.0, 20000),
        ("2026-01-02 23:00:00", 1300, 29000.0, 29000),
    ]),
    # inst 2 · q2 — 하루 중 카운터 리셋(재기동): last < first → 델타 0 클램프.
    (2, "2026-01-01", "q2", "SELECT 2", [
        ("2026-01-01 06:00:00", 500, 5000.0, 5000),
        ("2026-01-01 23:00:00", 100, 1000.0, 1000),
    ]),
    # inst 2 · q2 — 다음날 정상 증가(델타 양수). 마트에 두 번째 악화 후보.
    (2, "2026-01-02", "q2", "SELECT 2", [
        ("2026-01-02 06:00:00", 100, 1000.0, 1000),
        ("2026-01-02 23:00:00", 400, 12000.0, 12000),
    ]),
]

_SCHEMA = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("instance_id", pa.int64(), nullable=False),
    pa.field("captured_at", pa.timestamp("us"), nullable=False),
    pa.field("query_id", pa.string(), nullable=False),
    pa.field("query_text", pa.string(), nullable=True),
    pa.field("calls", pa.int64(), nullable=False),
    pa.field("total_time_ms", pa.float64(), nullable=False),
    pa.field("rows_examined", pa.int64(), nullable=False),
])


def build(out_dir: str) -> int:
    out = Path(out_dir)
    rid = 0
    files = 0
    # (instance_id, dt) 단위로 파티션 파일을 모은다.
    by_part: dict[tuple[int, str], dict] = {}
    for instance_id, dt, query_id, query_text, snaps in _ROWS:
        part = by_part.setdefault((instance_id, dt), {
            "id": [], "instance_id": [], "captured_at": [], "query_id": [],
            "query_text": [], "calls": [], "total_time_ms": [], "rows_examined": [],
        })
        for cap, calls, tt, rex in snaps:
            rid += 1
            part["id"].append(rid)
            part["instance_id"].append(instance_id)
            part["captured_at"].append(datetime.strptime(cap, "%Y-%m-%d %H:%M:%S"))
            part["query_id"].append(query_id)
            part["query_text"].append(query_text)
            part["calls"].append(calls)
            part["total_time_ms"].append(tt)
            part["rows_examined"].append(rex)

    for (instance_id, dt), cols in by_part.items():
        pdir = out / f"dt={dt}" / f"instance_id={instance_id}"
        pdir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_arrays(
            [
                pa.array(cols["id"], pa.int64()),
                pa.array(cols["instance_id"], pa.int64()),
                pa.array(cols["captured_at"], pa.timestamp("us")),
                pa.array(cols["query_id"], pa.string()),
                pa.array(cols["query_text"], pa.string()),
                pa.array(cols["calls"], pa.int64()),
                pa.array(cols["total_time_ms"], pa.float64()),
                pa.array(cols["rows_examined"], pa.int64()),
            ],
            schema=_SCHEMA,
        )
        pq.write_table(table, pdir / "part-000.parquet", compression="zstd")
        files += 1
    print(f"픽스처 생성: {files}개 파티션 파일, {rid}행 → {out}")
    return files


def build_size_fixture(out_dir: str) -> int:
    """size_snapshot 픽스처(Phase 13) — 용량 마트가 CI에서 MinIO 없이 돌게 한다.

    2일치 × 2인스턴스, 크기 절대값 + 하루 안 2관측(마지막 관측이 대푯값인지 검증 재료).
    """
    schema = pa.schema([
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
    ])
    rows = [
        # (dt, inst, captured_at, name, rows, data, index) — 하루 2관측(06시·23시), 마지막이 대푯값.
        ("2026-01-01", 1, "2026-01-01 06:00:00", "orders", 1000, 100_000, 10_000),
        ("2026-01-01", 1, "2026-01-01 23:00:00", "orders", 1100, 110_000, 11_000),
        ("2026-01-02", 1, "2026-01-02 23:00:00", "orders", 1200, 120_000, 12_000),
        ("2026-01-01", 2, "2026-01-01 23:00:00", "events", 5000, 500_000, 50_000),
        ("2026-01-02", 2, "2026-01-02 23:00:00", "events", 5100, 510_000, 51_000),
    ]
    out = Path(out_dir)
    by_part: dict[tuple[str, int], list] = {}
    for i, (dt, iid, cap, name, n, d, ix) in enumerate(rows, start=1):
        by_part.setdefault((dt, iid), []).append((i, iid, cap, name, n, d, ix))
    files = 0
    for (dt, iid), part in by_part.items():
        pdir = out / f"dt={dt}" / f"instance_id={iid}"
        pdir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_arrays([
            pa.array([r[0] for r in part], pa.int64()),
            pa.array([r[1] for r in part], pa.int64()),
            pa.array([datetime.strptime(r[2], "%Y-%m-%d %H:%M:%S") for r in part],
                     pa.timestamp("us")),
            pa.array(["table"] * len(part), pa.string()),
            pa.array([r[3] for r in part], pa.string()),
            pa.array([r[4] for r in part], pa.int64()),
            pa.array([r[5] for r in part], pa.int64()),
            pa.array([r[6] for r in part], pa.int64()),
            pa.array([None] * len(part), pa.int64()),
            pa.array([None] * len(part), pa.int64()),
            pa.array([None] * len(part), pa.int64()),
        ], schema=schema)
        pq.write_table(table, pdir / "part-000.parquet", compression="zstd")
        files += 1
    print(f"size 픽스처 생성: {files}개 파티션 파일 → {out}")
    return files


def build_aux_fixtures(base_dir: str) -> None:
    """wait_event·backup_run·plan_snapshot 최소 픽스처(Phase 14 소비층의 CI 재료)."""
    def write(out_dir, schema, arrays, dt, iid):
        pdir = Path(out_dir) / f"dt={dt}" / f"instance_id={iid}"
        pdir.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_arrays(arrays, schema=schema), pdir / "part-000.parquet",
                       compression="zstd")

    ts = lambda t: datetime.strptime(t, "%Y-%m-%d %H:%M:%S")  # noqa: E731
    wait_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False), pa.field("instance_id", pa.int64(), nullable=False),
        pa.field("captured_at", pa.timestamp("us"), nullable=False),
        pa.field("event_name", pa.string(), nullable=False), pa.field("category", pa.string(), nullable=True),
        pa.field("wait_count", pa.int64(), nullable=False), pa.field("total_ms", pa.float64(), nullable=False)])
    # 누적 기종 흉내: 하루 2관측, last-first 델타 = 50 / 500ms
    write(base_dir + "_wait", wait_schema, [
        pa.array([1, 2], pa.int64()), pa.array([1, 1], pa.int64()),
        pa.array([ts("2026-01-01 06:00:00"), ts("2026-01-01 23:00:00")], pa.timestamp("us")),
        pa.array(["io/file", "io/file"], pa.string()), pa.array(["IO", "IO"], pa.string()),
        pa.array([100, 150], pa.int64()), pa.array([1000.0, 1500.0], pa.float64())], "2026-01-01", 1)
    backup_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False), pa.field("instance_id", pa.int64(), nullable=False),
        pa.field("started_at", pa.timestamp("us"), nullable=False),
        pa.field("status", pa.string(), nullable=False), pa.field("backup_type", pa.string(), nullable=True),
        pa.field("duration_ms", pa.int64(), nullable=False), pa.field("detail", pa.string(), nullable=True),
        pa.field("location", pa.string(), nullable=True), pa.field("verify_status", pa.string(), nullable=True),
        pa.field("verified_at", pa.timestamp("us"), nullable=True),
        pa.field("remote_location", pa.string(), nullable=True)])
    write(base_dir + "_backup", backup_schema, [
        pa.array([1, 2], pa.int64()), pa.array([1, 1], pa.int64()),
        pa.array([ts("2026-01-01 01:00:00"), ts("2026-01-01 13:00:00")], pa.timestamp("us")),
        pa.array(["SUCCESS", "FAILED"], pa.string()), pa.array(["FULL", "LOG"], pa.string()),
        pa.array([1000, 2000], pa.int64()), pa.array([None, "err"], pa.string()),
        pa.array(["/b/1", None], pa.string()), pa.array(["VERIFIED", None], pa.string()),
        pa.array([ts("2026-01-01 02:00:00"), None], pa.timestamp("us")),
        pa.array(["s3://b/1", None], pa.string())], "2026-01-01", 1)
    plan_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False), pa.field("instance_id", pa.int64(), nullable=False),
        pa.field("query_id", pa.string(), nullable=False), pa.field("plan_hash", pa.string(), nullable=False),
        pa.field("plan_shape", pa.string(), nullable=True),
        pa.field("captured_at", pa.timestamp("us"), nullable=False)])
    write(base_dir + "_plan", plan_schema, [
        pa.array([1, 2], pa.int64()), pa.array([1, 1], pa.int64()),
        pa.array(["q1", "q1"], pa.string()), pa.array(["h1", "h2"], pa.string()),
        pa.array(["shape1", "shape2"], pa.string()),
        pa.array([ts("2026-01-01 03:00:00"), ts("2026-01-01 15:00:00")], pa.timestamp("us"))],
        "2026-01-01", 1)
    print(f"aux 픽스처 3종 생성 → {base_dir}_wait/_backup/_plan")


def register_source_view(duckdb_file: str, fixture_dir: str, size_fixture_dir: str) -> None:
    """dbt unit test가 introspect할 수 있도록 raw 소스들을 뷰로 등록한다.

    외부 read_parquet 소스는 물리 relation이 없어 dbt-duckdb가 컬럼을 못 읽는다.
    픽스처 parquet를 가리키는 뷰를 dbt가 쓰는 바로 그 파일에 미리 만들어 둔다.
    """
    import duckdb  # noqa: PLC0415 — 선택 경로에서만 필요.

    con = duckdb.connect(duckdb_file)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(
            "CREATE OR REPLACE VIEW raw.query_snapshot AS "
            f"SELECT * FROM read_parquet('{fixture_dir}/dt=*/instance_id=*/*.parquet', "
            "hive_partitioning = 1)"
        )
        con.execute(
            "CREATE OR REPLACE VIEW raw.size_snapshot AS "
            f"SELECT * FROM read_parquet('{size_fixture_dir}/dt=*/instance_id=*/*.parquet', "
            "hive_partitioning = 1)"
        )
        base = fixture_dir  # aux 픽스처는 base_dir 접미 규약(_wait/_backup/_plan)
        for tbl, suffix in (("wait_event_snapshot", "_wait"), ("backup_run", "_backup"),
                            ("plan_snapshot", "_plan")):
            con.execute(
                f"CREATE OR REPLACE VIEW raw.{tbl} AS "
                f"SELECT * FROM read_parquet('{base}{suffix}/dt=*/instance_id=*/*.parquet', "
                "hive_partitioning = 1)"
            )
    finally:
        con.close()
    print(f"raw 소스 5종 뷰 등록 → {duckdb_file}")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "/tmp/lh_fixture"
    build(target)
    size_target = target + "_size"
    build_size_fixture(size_target)
    build_aux_fixtures(target)
    if len(sys.argv) > 2:
        register_source_view(sys.argv[2], target, size_target)
