"""실패 알림 — 조용히 멈춘 파이프라인을 사람에게 알리는 webhook(Phase 6).

fail-closed 게이트(Phase 3)는 반쪽 데이터를 잘 막았지만, 막았다는 사실을 아무도
모른다는 구멍이 있었다. 차단은 시작이고 통보가 완성이다. 이 모듈은 Airflow
on_failure_callback에서 호출되어 실패 컨텍스트(dag/task/실행일/로그 URL/에러 요약)를
webhook URL로 HTTP POST 한다.

설계 원칙:
- 알림 실패가 파이프라인을 또 죽이면 안 된다 — 전 과정을 try/except로 감싸고,
  실패해도 로그만 남기고 삼킨다(알림은 best-effort, 파이프라인 상태는 불변).
- URL은 환경변수 ALERT_WEBHOOK_URL로 주입한다(Slack/Discord/사내 수신기 어디든
  POST를 받는 곳이면 됨). 미설정이면 no-op — 알림 없이도 파이프라인은 돈다.
- 의존성 0 — 표준 라이브러리 urllib만 쓴다(컨테이너에 패키지 추가 없음).
- Airflow 2.x의 on_failure_callback이 표준 경로다. SLA 콜백은 쓰지 않는다
  (버그가 많아 3.0에서 제거된 폐기 경로).

수동 발화 테스트:
    ALERT_WEBHOOK_URL=http://localhost:18808/alert python -m extract.alerts
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

log = logging.getLogger("alerts")

# webhook 수신처. 미설정이면 알림은 no-op(파이프라인은 알림 없이도 돈다).
WEBHOOK_URL_ENV = "ALERT_WEBHOOK_URL"
# 알림 자체가 파이프라인을 붙잡지 않도록 짧게 끊는다.
POST_TIMEOUT_SEC = float(os.getenv("ALERT_TIMEOUT_SEC", "5"))
# 에러 요약 길이 상한 — 전체 스택은 로그 URL로 보게 하고, 알림은 요약만.
ERROR_SUMMARY_MAX = 1000
# 알림 받은 사람이 "지금 화면은 어떤 상태인가"를 한 클릭에 확인할 대시보드(Phase 8).
# 미설정이면 필드는 None — 알림 자체는 그대로 나간다.
DASHBOARD_URL_ENV = "METABASE_DASHBOARD_URL"


def post_webhook(payload: dict) -> bool:
    """payload를 webhook URL로 POST 한다. 성공 여부만 반환, 예외는 절대 밖으로 안 새 나간다."""
    url = os.getenv(WEBHOOK_URL_ENV, "").strip()
    if not url:
        log.info("%s 미설정 — 알림 생략(no-op)", WEBHOOK_URL_ENV)
        return False
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=POST_TIMEOUT_SEC) as resp:
            log.info("알림 전송 완료 → %s (HTTP %s)", url, resp.status)
            return True
    except Exception:  # noqa: BLE001 — 알림 실패가 파이프라인을 또 죽이면 안 된다.
        log.exception("알림 전송 실패(무시하고 계속) → %s", url)
        return False


def notify_task_failure(context: dict) -> None:
    """Airflow on_failure_callback 진입점 — 실패 태스크의 컨텍스트를 webhook으로 보낸다.

    default_args["on_failure_callback"]에 걸어두면 어느 태스크가 죽어도
    (재시도 소진 후 최종 실패 시) 이 함수가 한 번 불린다.
    """
    try:
        ti = context.get("task_instance")
        exc = context.get("exception")
        payload = {
            "event": "airflow_task_failed",
            "dag_id": getattr(ti, "dag_id", None),
            "task_id": getattr(ti, "task_id", None),
            "logical_date": str(context.get("logical_date") or context.get("execution_date")),
            "try_number": getattr(ti, "try_number", None),
            "max_tries": getattr(ti, "max_tries", None),
            "log_url": getattr(ti, "log_url", None),
            "dashboard_url": os.getenv(DASHBOARD_URL_ENV) or None,
            "error": (str(exc) if exc else "unknown")[:ERROR_SUMMARY_MAX],
        }
        log.warning("태스크 실패 감지 — 알림 발화: %s.%s", payload["dag_id"], payload["task_id"])
        post_webhook(payload)
    except Exception:  # noqa: BLE001 — 콜백 자체가 예외를 던지면 안 된다.
        log.exception("실패 알림 콜백 내부 오류(무시)")


if __name__ == "__main__":
    # 수신기까지의 배선을 파이프라인 없이 점검하는 수동 발화.
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ok = post_webhook({"event": "manual_test", "message": "extract.alerts 수동 발화 테스트"})
    raise SystemExit(0 if ok else 1)
