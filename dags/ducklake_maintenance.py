"""ducklake_maintenance — DuckLake 주기 유지보수 DAG (@weekly, Phase 6).

DuckLake는 커밋마다 스냅샷을 쌓고 덮어쓰인 파일을 타임트래블용으로 남겨둔다.
스스로는 아무것도 지우지 않으므로, 방치하면 카탈로그(PG)와 스토리지(S3)가
단조 증가한다. 이 DAG가 매주 공식 권장 번들인 CHECKPOINT(스냅샷 만료 +
인라인 플러시 + 인접 파일 컴팩션)와 삭제 예약 파일 정리를 돌린다.

- 보존 기간은 DUCKLAKE_RETENTION(기본 '7 days') — 원천 스냅샷 보존 7일과 대칭.
  그보다 오래된 버전으로의 타임트래블은 포기하는 대신 용량이 유계가 된다.
- 유지보수는 현재 상태를 절대 바꾸지 않는다(행수 불변식은 모듈이 검사).
- 실패하면 다른 DAG와 같은 webhook으로 통보한다(on_failure_callback).
- 주기 실행이 며칠 밀려도 다음 실행이 밀린 몫까지 정리하므로 catchup은 불필요.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pendulum

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from airflow.decorators import dag, task  # noqa: E402

from extract.alerts import notify_task_failure  # noqa: E402


@dag(
    dag_id="ducklake_maintenance",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 7, 6, tz="UTC"),  # 일요일 자정(UTC) 경계 정렬.
    catchup=False,
    max_active_runs=1,
    default_args={
        # 카탈로그 PG·S3 순단은 재시도로 흡수. 유지보수는 멱등이라 재시도가 안전하다.
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": pendulum.duration(minutes=30),
        "on_failure_callback": notify_task_failure,
    },
    tags=["lakehouse", "ducklake", "maintenance"],
    doc_md=__doc__,
)
def ducklake_maintenance():
    @task
    def checkpoint() -> dict:
        """CHECKPOINT 번들 실행. 전/후 지표(스냅샷·파일·바이트)를 XCom으로 남긴다."""
        from extract.ducklake_maintenance import print_report, run_maintenance

        result = run_maintenance()
        print_report(result)
        return result

    checkpoint()


ducklake_maintenance()
