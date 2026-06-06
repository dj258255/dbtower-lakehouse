"""snapshot_offload — query_snapshot 일 배치 파이프라인 DAG.

    offload(EL) → quality_gate(품질 검문) → transform(dbt run+test)

매일 UTC 새벽, 논리 날짜(data_interval_start의 날짜 = '어제')의 스냅샷을
메타 PG에서 읽어 MinIO에 parquet로 내리고(Phase 1), 품질 게이트로 검문한 뒤
(Phase 3), 통과하면 컨테이너 안의 dbt로 변환·테스트까지 돌린다(Phase 6).
핵심 로직은 extract 패키지에 있고 이 DAG는 얇게 감쌀 뿐이다(=Airflow 없이도 재현 가능).

fail-closed(Phase 3): quality_gate는 정합·완결성·신선도를 검문하고, FAIL이면 예외를
던진다. 그러면 태스크 의존성상 downstream transform은 실행되지 않는다(upstream_failed).
조용히 틀린 반쪽 데이터 위에 마트를 짓지 않는다.

운영 경화(Phase 6):
- 실패 알림: default_args의 on_failure_callback이 어떤 태스크든 최종 실패 시
  webhook(ALERT_WEBHOOK_URL)으로 통보한다. fail-closed는 차단까지고, 통보가 완성이다.
  (Airflow 2.x 표준 경로. SLA 콜백은 폐기 경로라 쓰지 않는다.)
- retry 정책: 추출은 일시 장애(네트워크·원천 재기동)가 흔하므로 retries=3 +
  지수 백오프. 단 quality_gate는 retries=0 — 품질 FAIL은 결정적이라 재시도해도
  그대로 FAIL이고, 재시도는 원천/S3 재검문 부하만 늘린다.
- transform 완결: 컨테이너 안 별도 venv(/opt/dbt-venv)의 dbt가 run+test를 실제
  실행한다. Airflow 의존성과 dbt 의존성이 충돌하지 않도록 venv를 분리했다(Dockerfile).

함정 방어(docs/ROADMAP.md Phase 0·1):
- start_date를 @daily 경계(자정)에 맞춰 첫 인터벌 어긋남을 막는다.
- catchup=False 유지(무의도 대량 백필 방지). 과거 재적재는 명시적 backfill로만
  (절차는 docs/RUNBOOK.md).
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

from extract.alerts import notify_task_failure  # noqa: E402

# 컨테이너 안 dbt 실행 경로. Dockerfile이 Airflow와 분리된 venv에 dbt-duckdb를 깐다.
DBT_BIN = "/opt/dbt-venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/dbtower_lakehouse"


@dag(
    dag_id="snapshot_offload",
    schedule="@daily",
    # @daily는 자정 경계. start_date도 자정으로 맞춰 첫 data interval이 어긋나지 않게 한다.
    start_date=pendulum.datetime(2026, 7, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        # 추출·변환의 일시 장애(네트워크 순단, 원천 재기동)는 재시도로 흡수한다.
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=2),
        # 2분 → 4분 → 8분 지수 백오프. 원천이 살아나는 시간을 벌어준다.
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=30),
        # 어느 태스크든 최종 실패하면 webhook으로 통보(알림 실패는 삼킴 — alerts.py).
        "on_failure_callback": notify_task_failure,
    },
    tags=["lakehouse", "extract", "el", "quality", "transform"],
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
        의존하는 transform은 upstream_failed가 되어 실행되지 않는다. 그리고
        on_failure_callback이 webhook으로 "막았다"는 사실을 사람에게 알린다.
        """
        from extract.quality import assert_gate

        dt = offload_result["dt"]
        assert_gate([dt])  # FAIL 있으면 RuntimeError → 태스크 실패 → transform 차단
        return {"dt": dt, "gate": "PASS"}

    @task
    def transform(gate_result: dict) -> dict:
        """dbt 변환+테스트 — 게이트를 통과했을 때만, 컨테이너 안에서 실제 실행한다.

        Phase 6 이전에는 컨테이너에 dbt가 없어 이 태스크가 로그만 남기고 실제 빌드는
        호스트 수동 실행에 의존했다 — 오케스트레이션의 최대 구멍. 이제 Dockerfile이
        분리 venv(/opt/dbt-venv)에 dbt-duckdb를 얹어, run과 test가 전부 이 태스크
        안에서 돈다. 모델 빌드가 되어도 테스트가 깨지면 태스크는 실패다.
        """
        import subprocess

        dt = gate_result["dt"]
        results: dict = {"dt": dt}
        for command in ("run", "test"):
            proc = subprocess.run(
                [
                    DBT_BIN, command,
                    "--profiles-dir", DBT_PROJECT_DIR,
                    "--project-dir", DBT_PROJECT_DIR,
                ],
                capture_output=True,
                text=True,
                timeout=1800,
            )
            print(proc.stdout)
            if proc.stderr:
                print(proc.stderr, file=sys.stderr)
            if proc.returncode != 0:
                raise RuntimeError(f"dbt {command} 실패 (exit {proc.returncode}) — 위 로그 참조")
            results[f"dbt_{command}"] = "PASS"
        return results

    transform(quality_gate(offload()))


snapshot_offload()
