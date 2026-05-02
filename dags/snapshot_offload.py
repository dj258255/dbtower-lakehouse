"""snapshot_offload — query_snapshot 일 배치 파이프라인 DAG.

    offload(EL) → quality_gate(품질 검문) → transform(dbt)

매일 UTC 새벽, 논리 날짜(data_interval_start의 날짜 = '어제')의 스냅샷을
메타 PG에서 읽어 MinIO에 parquet로 내리고(Phase 1), 다운스트림 변환으로 넘기기 전에
품질 게이트로 검문한다(Phase 4). 핵심 로직은 extract 패키지에 있고 이 DAG는 얇게 감쌀
뿐이다(=Airflow 없이도 e2e 재현 가능).

fail-closed(Phase 4): quality_gate는 정합·완결성·신선도를 검문하고, FAIL이면 예외를
던진다. 그러면 태스크 의존성상 downstream transform은 실행되지 않는다(upstream_failed).
조용히 틀린 반쪽 데이터 위에 마트를 짓지 않는다.

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
    tags=["lakehouse", "extract", "el", "quality"],
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

    @task(retries=0)  # 품질 FAIL은 결정적이다 — 재시도해도 그대로 FAIL이므로 즉시 차단한다.
    def quality_gate(offload_result: dict) -> dict:
        """품질 게이트 — 정합·완결성·신선도. FAIL이면 예외를 던져 downstream을 막는다.

        fail-closed의 심장. 여기서 raise하면 Airflow가 이 태스크를 failed로 표시하고,
        의존하는 transform은 upstream_failed가 되어 실행되지 않는다.
        """
        from extract.quality import assert_gate

        dt = offload_result["dt"]
        assert_gate([dt])  # FAIL 있으면 RuntimeError → 태스크 실패 → transform 차단
        return {"dt": dt, "gate": "PASS"}

    @task
    def transform(gate_result: dict) -> str:
        """dbt 변환 — 게이트를 통과했을 때만 도달한다.

        컨테이너에는 dbt를 얹지 않았다(추출·게이트만 in-container). 실제 dbt 빌드는
        호스트의 run_pipeline(게이트→dbt run)에서 실측한다. 이 태스크의 역할은
        '게이트 통과가 전제되어야만 변환이 시작된다'는 오케스트레이션 계약의 증명이다.
        """
        import logging

        log = logging.getLogger("transform")
        log.info("품질 게이트 통과(dt=%s) 확인 → dbt 변환 단계 진입", gate_result["dt"])
        return f"transform ready for dt={gate_result['dt']} (dbt run은 호스트 run_pipeline에서 실측)"

    transform(quality_gate(offload()))


snapshot_offload()
