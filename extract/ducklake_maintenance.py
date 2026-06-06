"""DuckLake 주기 유지보수 — 방치하면 스냅샷·옛 파일이 무한 증가한다(Phase 6).

DuckLake는 커밋마다 스냅샷(버전)을 카탈로그에 쌓고, 덮어쓰인·삭제된 데이터 파일도
과거 버전 타임트래블을 위해 S3에 남겨둔다. 타임트래블의 대가다 — 스스로는 아무것도
지우지 않으므로, 주기 유지보수 없이는 카탈로그와 스토리지가 단조 증가한다.

정리 수단은 공식 권장인 **CHECKPOINT 번들**을 쓴다. 스냅샷 만료·인라인 데이터
플러시·인접 파일 컴팩션을 안전한 순서로 한 번에 묶어 준다 — 만료와 컴팩션을 손으로
따로 부르면 순서에 따라 파일이 남거나 스냅샷이 꼬이는 이슈가 보고돼 있다
(ducklake #336/#536). 만료 기준은 ATTACH 옵션 `expire_older_than`(보존 기간)이고,
만료로 '삭제 예약'된 파일의 실제 삭제는 `ducklake_cleanup_old_files`가 마무리한다.

    python -m extract.ducklake_maintenance                      # 보존 7일(운영 기본)
    python -m extract.ducklake_maintenance --retention '0 seconds'  # 즉시 만료(데모)

Airflow에선 @weekly DAG(dags/ducklake_maintenance.py)가 이 모듈을 감싼다.
"""
from __future__ import annotations

import argparse
import logging
import os
import re

import boto3
from botocore.client import Config as BotoConfig

from extract.config import DuckLakeConfig, SinkConfig
from extract.ducklake_load import TABLE_NAME, open_lake

log = logging.getLogger("ducklake_maintenance")

# 보존 기간 — 이보다 오래된 스냅샷은 만료(타임트래블 불가)되고 파일이 정리 대상이 된다.
# raw parquet(원본)가 따로 있으므로 DuckLake 스냅샷은 7일이면 충분하다(원천 보존 7일과 대칭).
DEFAULT_RETENTION = os.getenv("DUCKLAKE_RETENTION", "7 days")

# retention은 SQL에 리터럴로 들어가므로 형태를 강하게 검증한다(주입 차단).
_RETENTION_RE = re.compile(
    r"^\d+\s+(second|minute|hour|day|week|month|year)s?$", re.IGNORECASE
)


def _s3_client(sink: SinkConfig):
    return boto3.client(
        "s3",
        endpoint_url=sink.endpoint,
        aws_access_key_id=sink.access_key,
        aws_secret_access_key=sink.secret_key,
        region_name=sink.region,
        config=BotoConfig(signature_version="s3v4", connect_timeout=5, read_timeout=60),
    )


def _s3_stats(s3, sink: SinkConfig, cfg: DuckLakeConfig) -> tuple[int, int]:
    """DuckLake DATA_PATH 아래 실제 오브젝트 (개수, 총 바이트)."""
    prefix = cfg.data_path.replace(f"s3://{sink.bucket}/", "")
    count, size = 0, 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=sink.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            count += 1
            size += obj["Size"]
    return count, size


def measure(con, cfg: DuckLakeConfig, s3, sink: SinkConfig) -> dict:
    """유지보수 전/후를 대조할 지표 — 스냅샷 수·활성 파일 수·S3 오브젝트 수·바이트."""
    snapshots = con.execute(
        f"SELECT count(*) FROM ducklake_snapshots('{cfg.lake_alias}')"
    ).fetchone()[0]
    # 카탈로그가 '현재 살아있다'고 보는 데이터 파일(만료로 떨어져 나간 것 제외).
    active_files = con.execute(
        f"SELECT count(*) FROM ducklake_list_files('{cfg.lake_alias}', '{TABLE_NAME}')"
    ).fetchone()[0]
    s3_objects, s3_bytes = _s3_stats(s3, sink, cfg)
    rows = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    return {
        "snapshots": snapshots,
        "active_data_files": active_files,
        "s3_objects": s3_objects,
        "s3_bytes": s3_bytes,
        "table_rows": rows,
    }


def run_maintenance(retention: str | None = None) -> dict:
    """CHECKPOINT 번들 + 삭제 예약 파일 정리. 전/후 지표를 함께 반환한다(증거용).

    순서(공식 권장 그대로):
      1) expire_older_than 옵션으로 보존 기간을 선언
      2) CHECKPOINT — 만료 + 인라인 플러시 + 인접 파일 컴팩션을 안전한 순서로 번들 실행
      3) ducklake_cleanup_old_files — 만료로 '삭제 예약'된 파일의 실제 S3 삭제
    """
    retention = (retention or DEFAULT_RETENTION).strip()
    if not _RETENTION_RE.match(retention):
        raise ValueError(f"retention 형식 오류: {retention!r} (예: '7 days', '0 seconds')")

    cfg, sink = DuckLakeConfig(), SinkConfig()
    s3 = _s3_client(sink)
    con = open_lake(cfg, sink)
    try:
        before = measure(con, cfg, s3, sink)
        log.info("유지보수 전: %s", before)

        # 1) 보존 기간 선언 — 이보다 오래된 스냅샷은 만료 대상.
        con.execute(f"CALL {cfg.lake_alias}.set_option('expire_older_than', '{retention}')")

        # 2) 공식 번들 — 만료·플러시·컴팩션을 안전한 순서로 한 번에.
        con.execute(f"CHECKPOINT {cfg.lake_alias}")

        # 3) 만료가 '삭제 예약'한 파일을 실제로 지운다(예약만으로는 S3 용량이 안 준다).
        removed = con.execute(
            f"CALL ducklake_cleanup_old_files('{cfg.lake_alias}', cleanup_all => true)"
        ).fetchall()

        after = measure(con, cfg, s3, sink)
        log.info("유지보수 후: %s (파일 %d개 삭제)", after, len(removed))

        result = {
            "retention": retention,
            "before": before,
            "after": after,
            "files_removed": len(removed),
        }
        # 안전 불변식 — 유지보수는 현재 상태(행수)를 절대 바꾸면 안 된다.
        if before["table_rows"] != after["table_rows"]:
            raise RuntimeError(
                f"유지보수 후 행수 변동 {before['table_rows']} → {after['table_rows']} — 즉시 조사 필요"
            )
        return result
    finally:
        con.close()


def print_report(result: dict) -> None:
    b, a = result["before"], result["after"]
    print(f"[DuckLake 유지보수] retention = {result['retention']}")
    print(f"{'지표':<22} {'전':>14} {'후':>14}")
    print("-" * 54)
    print(f"{'스냅샷 수':<22} {b['snapshots']:>14,} {a['snapshots']:>14,}")
    print(f"{'활성 데이터 파일':<22} {b['active_data_files']:>14,} {a['active_data_files']:>14,}")
    print(f"{'S3 오브젝트 수':<22} {b['s3_objects']:>14,} {a['s3_objects']:>14,}")
    print(f"{'S3 바이트':<22} {b['s3_bytes']:>14,} {a['s3_bytes']:>14,}")
    print(f"{'테이블 행수(불변식)':<22} {b['table_rows']:>14,} {a['table_rows']:>14,}")
    print(f"삭제된 파일: {result['files_removed']}개")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="DuckLake CHECKPOINT 유지보수")
    parser.add_argument("--retention", default=None, help="보존 기간(기본: DUCKLAKE_RETENTION 또는 '7 days')")
    args = parser.parse_args()
    print_report(run_maintenance(args.retention))
