"""deadman 감시 — heartbeat가 기한 내 안 찍히면 경보하는 역방향 감시 (Phase 9).

extract/heartbeat.py가 "성공했다"를 주기적으로 남긴다면, 이 모듈은 그 신호가
**끊긴 것**을 잡는다. 실패를 감지하는 게 아니라 '성공의 부재'를 감지한다 —
그래서 태스크가 아예 시작조차 못 한 경우(스케줄러 death·DAG pause·원천 수집기
침묵)까지 잡힌다. on_failure_callback으로는 절대 못 잡는 구멍이다.

두 가지 방식으로 돌린다:
  1) Airflow DAG(dags/deadman_watch.py, @hourly) — 스케줄러가 살아 있는 동안
     DAG pause·업스트림 미실행·연속 실패로 heartbeat가 낡는 것을 잡는다.
  2) 완전 외부 실행(host cron·systemd timer·별도 컨테이너) — `python -m extract.deadman`.
     Airflow 스케줄러가 통째로 죽으면 (1)도 같이 죽으므로, 진짜 'total death'를
     잡으려면 감시자는 감시 대상 밖에 있어야 한다. 정직한 한계: 같은 Airflow 안의
     감시 DAG는 자기 스케줄러의 죽음은 못 잡는다.

경보는 기존 webhook(extract/alerts.post_webhook)을 재사용한다 — 채널·수신기·배선이
실패 알림과 동일하다(서비스 추가 0). 종료코드 = 경보 발화 수(0=건강)라 외부 cron이
비영 종료를 자체 경보로도 태울 수 있다.

    python -m extract.deadman                    # 기본 감시(snapshot_offload)
    DEADMAN_WATCH="snapshot_offload:26h,ducklake_maintenance:8d" python -m extract.deadman
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime

from extract.alerts import post_webhook
from extract.heartbeat import read_heartbeats

log = logging.getLogger("deadman")

# 감시 대상 기본값 — @daily DAG는 24h 주기라 26h(2h 유예) 넘게 성공이 없으면 낡음.
DEFAULT_WATCH = os.getenv("DEADMAN_WATCH", "snapshot_offload:26h")

_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> int:
    """'26h' / '90m' / '8d' / '3600s' → 초. 접미사 없으면 시간으로 본다."""
    text = text.strip().lower()
    if not text:
        raise ValueError("빈 기간")
    unit = text[-1]
    if unit in _UNIT_SECONDS:
        return int(float(text[:-1]) * _UNIT_SECONDS[unit])
    return int(float(text) * 3600)  # 접미사 없음 → 시간.


def parse_watch(spec: str) -> dict[str, int]:
    """'dag:26h,dag2:8d' → {dag: max_age_seconds}."""
    out: dict[str, int] = {}
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, dur = item.partition(":")
        out[name.strip()] = parse_duration(dur or "26h")
    return out


@dataclass
class StaleVerdict:
    dag_id: str
    stale: bool
    age_seconds: float | None  # None = heartbeat가 아예 없음(한 번도 성공 못 함).
    max_age_seconds: int
    last_success_at: datetime | None

    @property
    def age_hours(self) -> float | None:
        return None if self.age_seconds is None else self.age_seconds / 3600.0


def check_stale(
    dag_id: str,
    last_success_at: datetime | None,
    max_age_seconds: int,
    now: datetime | None = None,
) -> StaleVerdict:
    """heartbeat 하나의 신선도 판정(순수 로직 — 테스트 대상).

    - heartbeat 없음(None)            → stale (한 번도 성공 못 함 / 테이블 비어 있음)
    - now - last_success > max_age    → stale (기한 초과)
    - 그 외                           → 건강
    """
    now = now or datetime.now(UTC)
    if last_success_at is None:
        return StaleVerdict(dag_id, True, None, max_age_seconds, None)
    if last_success_at.tzinfo is None:
        last_success_at = last_success_at.replace(tzinfo=UTC)
    age = (now - last_success_at).total_seconds()
    return StaleVerdict(dag_id, age > max_age_seconds, age, max_age_seconds, last_success_at)


def evaluate(
    watch: dict[str, int],
    heartbeats: dict[str, datetime],
    now: datetime | None = None,
) -> list[StaleVerdict]:
    """감시 대상 전부에 대해 신선도 판정 리스트를 낸다(순수 — 테스트 대상)."""
    now = now or datetime.now(UTC)
    return [
        check_stale(dag, heartbeats.get(dag), max_age, now)
        for dag, max_age in watch.items()
    ]


def _alert_payload(v: StaleVerdict) -> dict:
    if v.age_seconds is None:
        reason = "heartbeat 없음 — 한 번도 성공 기록이 없다(DAG 미실행/pause 의심)"
        age_h = None
    else:
        reason = (
            f"heartbeat 정지 {v.age_hours:.1f}h(기한 {v.max_age_seconds / 3600:.1f}h 초과) "
            "— 스케줄러 death/pause/원천 침묵 의심"
        )
        age_h = round(v.age_hours, 2)
    return {
        "event": "pipeline_deadman",
        "dag_id": v.dag_id,
        "last_success_at": v.last_success_at.isoformat() if v.last_success_at else None,
        "age_hours": age_h,
        "deadline_hours": round(v.max_age_seconds / 3600.0, 2),
        "error": reason,
    }


def run(watch_spec: str | None = None, now: datetime | None = None) -> int:
    """감시 1회 — 낡은 heartbeat마다 webhook 경보를 쏘고 발화 수를 반환한다."""
    watch = parse_watch(watch_spec or DEFAULT_WATCH)
    heartbeats = read_heartbeats()
    verdicts = evaluate(watch, heartbeats, now)

    fired = 0
    for v in verdicts:
        if v.stale:
            fired += 1
            payload = _alert_payload(v)
            log.warning("deadman 경보 — %s: %s", v.dag_id, payload["error"])
            post_webhook(payload)
        else:
            log.info(
                "heartbeat 건강 — %s: %.1fh 전 성공(기한 %.1fh)",
                v.dag_id, v.age_hours, v.max_age_seconds / 3600.0,
            )
    if fired == 0:
        log.info("deadman: 감시 대상 %d개 전부 건강", len(verdicts))
    return fired


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spec = sys.argv[1] if len(sys.argv) > 1 else None
    raise SystemExit(run(spec))
