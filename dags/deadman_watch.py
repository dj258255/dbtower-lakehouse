"""deadman_watch — heartbeat가 낡으면 경보하는 역방향 감시 DAG (@hourly, Phase 9).

기존 알림은 "실패하면 운다"뿐이라, 태스크가 아예 시작조차 못 하는 경우(DAG pause·
업스트림 미실행·원천 수집기 침묵)엔 아무도 울지 않는다. 이 DAG는 snapshot_offload의
성공 heartbeat(extract/heartbeat.py가 마지막 태스크에서 기록)를 매시간 확인하고,
기한(기본 26h) 넘게 갱신이 없으면 같은 webhook 채널로 경보한다(extract/deadman.py).

정직한 한계: 이 감시 DAG도 같은 Airflow 스케줄러 위에서 돈다. 스케줄러가 통째로
죽으면 이 DAG도 같이 죽어 '완전한 침묵'은 못 잡는다. 그 경우를 잡으려면 감시자는
감시 대상 밖에 있어야 한다 — host cron/systemd timer로 `python -m extract.deadman`을
Airflow 밖에서 돌리는 경로를 RUNBOOK에 함께 문서화했다. 이 DAG는 스케줄러가 살아
있는 동안의 pause·연속 실패·원천 침묵을 잡는 1차 방어선이다.

- schedule=@hourly, catchup=False(밀린 시간을 몰아 감시할 이유 없음 — 지금이 중요).
- 감시 대상·기한은 DEADMAN_WATCH 환경변수(예: "snapshot_offload:26h").
- 감시 태스크 자체의 실패(카탈로그 PG 순단 등)는 다른 DAG와 같은 on_failure_callback으로.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pendulum

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from airflow.decorators import dag, task  # noqa: E402

from extract.alerts import notify_task_failure  # noqa: E402


@dag(
    dag_id="deadman_watch",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 7, 9, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=2),
        "on_failure_callback": notify_task_failure,
    },
    tags=["lakehouse", "deadman", "monitoring"],
    doc_md=__doc__,
)
def deadman_watch():
    @task
    def check() -> dict:
        """heartbeat 신선도 1회 점검 — 낡았으면 deadman.run이 webhook 경보를 쏜다."""
        from extract.deadman import run

        fired = run()
        return {"alerts_fired": fired}

    check()


deadman_watch()
