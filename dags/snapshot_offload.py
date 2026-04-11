"""snapshot_offload — query_snapshot 일 배치 Extract & Load DAG.

매일 UTC 새벽, 논리 날짜(data_interval_start의 날짜 = '어제')의 스냅샷을
메타 PG에서 읽어 MinIO에 parquet로 내린다. 핵심 로직은 extract.offload에 있고
이 DAG는 그것을 얇게 감쌀 뿐이다(=Airflow 없이도 e2e 검증 가능).

함정 방어(docs/ROADMAP.md Phase 0·1):
- start_date를 @daily 경계(자정)에 맞춰 첫 인터벌 어긋남을 막는다.
- catchup=False로 시작(무의도 대량 백필 방지).
- max_active_runs=1 + 태스크 동시성 상한으로 backfill이 스케줄러를 짓누르지 않게 한다.
- 멱등: run_offload가 파티션을 통째 덮어쓰므로 같은 날짜 재실행해도 중복 0.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pendulum

# 프로젝트 루트를 path에 넣어 extract 패키지를 import (컨테이너에선 /opt/airflow가 루트).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from airflow.decorators import dag, task  # noqa: E402


@dag(
    dag_id="snapshot_offload",
    schedule="@daily",
    # @daily는 자정 경계. start_date도 자정으로 맞춰 첫 data interval이 어긋나지 않게 한다.
    start_date=pendulum.datetime(2026, 7, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["lakehouse", "extract", "el"],
    doc_md=__doc__,
)
def snapshot_offload():
    @task(
        # backfill 시에도 한 번에 이 태스크가 여러 개 안 뜨게 상한(자원 짓눌림 방지).
        max_active_tis_per_dag=1,
    )
    def offload(data_interval_start: datetime | None = None) -> dict:
        from extract.offload import run_offload

        # data_interval_start의 날짜 = 이 실행이 담당하는 논리 날짜(어제).
        logical_day = data_interval_start.date().isoformat()
        return run_offload(logical_day)

    offload()


snapshot_offload()
