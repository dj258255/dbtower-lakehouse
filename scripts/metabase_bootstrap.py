"""Metabase 부트스트랩 — 초기 설정부터 대시보드까지 API로 재현 (Phase 7).

대시보드를 손으로 만들면 "만든 사람 브라우저에만 있는 산출물"이 된다. 이 스크립트는
빈 Metabase에서 출발해 아래를 전부 REST API로 만든다(멱등 — 이름으로 찾아 있으면 재사용).

  1. 초기 설정(관리자 계정) — .env의 METABASE_ADMIN_EMAIL / METABASE_ADMIN_PASSWORD
  2. DuckDB 커넥션: DuckLake(카탈로그=PG, 데이터=S3)를 read-only로 ATTACH
     - dbt의 DuckDB "파일"이 아니라 DuckLake에 붙는 이유: 파일은 프로세스 간
       단일 쓰기라 BI가 물면 transform과 충돌한다(잠금 충돌, 컨테이너 경계에선
       잠금 소실 — docs/VERIFICATION.md 8절 실측). DuckLake는 읽기/쓰기가 안 막는다.
  3. 질문 3개: 악화 쿼리 랭킹(표) · 일별 호출량 추이(선) · 지연 증가 상위 합계(숫자)
  4. 대시보드 1장 "지난 구간보다 느려진 쿼리 있어?" + 인스턴스 필터(질문들에 배선)

사용(호스트):
    docker compose up -d metabase
    .venv/bin/python scripts/metabase_bootstrap.py

주의: Metabase는 도커 네트워크(dbtower_default) 안에서 카탈로그 PG·MinIO를 컨테이너
호스트명으로 본다 — 접속 정보의 기본값이 localhost가 아니라 dbtower-postgres /
dbtower-minio 인 이유.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------- 환경
def load_env() -> dict[str, str]:
    """.env(있으면)를 읽고 os.environ이 우선하도록 합친다. source 안 해도 되게."""
    env: dict[str, str] = {}
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                env[key.strip()] = value.strip()
    env.update(os.environ)
    return env


ENV = load_env()

MB_URL = ENV.get("METABASE_URL", "http://localhost:13001")
ADMIN_EMAIL = ENV.get("METABASE_ADMIN_EMAIL", "admin@dbtower.local")
ADMIN_PASSWORD = ENV.get("METABASE_ADMIN_PASSWORD", "")

# Metabase 컨테이너 관점의 접속 정보(도커 네트워크 호스트명).
LAKE_PG_HOST = ENV.get("METABASE_LAKE_PG_HOST", "dbtower-postgres")
LAKE_PG_PORT = ENV.get("METABASE_LAKE_PG_PORT", "5432")
LAKE_PG_DB = ENV.get("DUCKLAKE_CATALOG_DB", "ducklake_catalog")
LAKE_PG_USER = ENV.get("SRC_PG_USER", "postgres")
LAKE_PG_PASSWORD = ENV.get("SRC_PG_PASSWORD", "dbtower1234")
S3_HOSTPORT = ENV.get("METABASE_S3_HOSTPORT", "dbtower-minio:9000")
S3_ACCESS_KEY = ENV.get("S3_ACCESS_KEY", "dbtower")
S3_SECRET_KEY = ENV.get("S3_SECRET_KEY", "dbtower1234")
S3_REGION = ENV.get("S3_REGION", "us-east-1")

DB_NAME = "lakehouse (DuckLake)"
DASHBOARD_NAME = "지난 구간보다 느려진 쿼리 있어?"


# ---------------------------------------------------------------- API 헬퍼
def api(method: str, path: str, body: dict | None = None, token: str | None = None):
    request = urllib.request.Request(
        MB_URL + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Content-Type": "application/json",
            **({"X-Metabase-Session": token} if token else {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")[:500]
        raise RuntimeError(f"{method} {path} → HTTP {error.code}: {detail}") from error


def wait_healthy(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            if api("GET", "/api/health").get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(f"Metabase가 {timeout_s}s 안에 안 떴다 — docker logs lakehouse-metabase")


# ---------------------------------------------------------------- 단계
def ensure_setup() -> str:
    """초기 설정(필요 시) 후 세션 토큰을 반환한다."""
    if not ADMIN_PASSWORD:
        sys.exit("METABASE_ADMIN_PASSWORD가 비어 있다 — .env를 채울 것(.env.example 참조)")
    # 이미 설정돼 있으면 로그인만 한다(setup-token은 설정 후에도 내려올 수 있다 — 실측).
    try:
        session = api("POST", "/api/session",
                      {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
        return session["id"]
    except RuntimeError:
        pass
    setup_token = api("GET", "/api/session/properties").get("setup-token")
    if setup_token:
        api("POST", "/api/setup", {
            "token": setup_token,
            "user": {
                "email": ADMIN_EMAIL, "password": ADMIN_PASSWORD,
                "first_name": "DBTower", "last_name": "Admin",
                "site_name": "dbtower-lakehouse",
            },
            "prefs": {"site_name": "dbtower-lakehouse", "site_locale": "ko"},
        })
        print(f"[setup] 관리자 계정 생성: {ADMIN_EMAIL}")
    session = api("POST", "/api/session",
                  {"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    return session["id"]


def ensure_database(token: str) -> int:
    """DuckLake 커넥션을 만들고(있으면 재사용) 스키마 동기화까지 기다린다."""
    for db in api("GET", "/api/database", token=token).get("data", []):
        if db["name"] == DB_NAME:
            print(f"[database] 기존 재사용: {DB_NAME} (id={db['id']})")
            return db["id"]

    details = {
        # DuckLake 카탈로그(PG)를 DuckDB의 데이터베이스로 직접 연다.
        "database_file": (
            f"ducklake:postgres:dbname={LAKE_PG_DB} host={LAKE_PG_HOST} "
            f"port={LAKE_PG_PORT} user={LAKE_PG_USER} password={LAKE_PG_PASSWORD} "
            f"connect_timeout=5"
        ),
        # 대시보드는 읽기만 한다 — 쓰기는 파이프라인(publish 태스크)의 몫.
        "read_only": True,
        # 새 커넥션마다 실행: 데이터 파일(S3/MinIO) 자격증명.
        #
        # 함정(실측): CREATE OR REPLACE SECRET을 쓰면 안 된다. 대시보드가 카드
        # 3장을 동시에 쏘면 커넥션 풀이 커넥션을 여러 개 열고, init_sql이 같은
        # DuckDB 인스턴스의 공유 카탈로그에 SECRET replace를 동시에 시도해
        # "Catalog write-write conflict on alter with minio"로 카드가 500이 난다.
        # SET s3_*는 커넥션(세션) 로컬이라 경합 자체가 없다. 확장(ducklake·
        # postgres·httpfs)은 autoinstall/autoload가 알아서 얹는다(INSTALL 불필요).
        "init_sql": (
            f"SET s3_endpoint='{S3_HOSTPORT}'; "
            f"SET s3_access_key_id='{S3_ACCESS_KEY}'; "
            f"SET s3_secret_access_key='{S3_SECRET_KEY}'; "
            "SET s3_url_style='path'; SET s3_use_ssl=false; "
            f"SET s3_region='{S3_REGION}';"
        ),
    }
    db = api("POST", "/api/database", token=token,
             body={"engine": "duckdb", "name": DB_NAME, "details": details})
    db_id = db["id"]
    print(f"[database] 생성: {DB_NAME} (id={db_id}) — 동기화 대기")
    api("POST", f"/api/database/{db_id}/sync_schema", token=token)
    for _ in range(60):
        time.sleep(2)
        if api("GET", f"/api/database/{db_id}", token=token).get("initial_sync_status") == "complete":
            break
    tables = [t["name"] for t in api("GET", f"/api/database/{db_id}/metadata", token=token)["tables"]]
    print(f"[database] 테이블: {sorted(tables)}")
    missing = {"fct_query_daily", "mart_query_regression"} - set(tables)
    if missing:
        raise RuntimeError(f"마트가 안 보인다: {missing} — publish 태스크가 돌았는지 확인")
    return db_id


INSTANCE_TAG = {
    "id": str(uuid.uuid4()),
    "name": "instance_id",
    "display-name": "Instance",
    "type": "number",
    "required": False,
}

REGRESSION_SQL = """\
select
    instance_id,
    substr(query_id, 1, 20)        as query_id,
    recent_from_dt,
    recent_to_dt,
    round(prior_latency_ms, 2)     as prior_avg_latency_ms,
    round(recent_latency_ms, 2)    as recent_avg_latency_ms,
    round(latency_increase_ms, 2)  as increase_ms,
    latency_increase_pct           as increase_pct,
    recent_delta_calls,
    substr(regexp_replace(query_text, '\\s+', ' ', 'g'), 1, 60) as query
from mart_query_regression
where 1 = 1
  [[and instance_id = {{instance_id}}]]
order by latency_increase_ms desc\
"""

TREND_SQL = """\
select
    dt,
    sum(delta_calls)                    as calls,
    round(sum(delta_total_time_ms), 0)  as total_time_ms
from fct_query_daily
where 1 = 1
  [[and instance_id = {{instance_id}}]]
group by dt
order by dt\
"""

WORST_SQL = """\
select count(*) as regressed_queries
from mart_query_regression
where 1 = 1
  [[and instance_id = {{instance_id}}]]\
"""

# ---------------------------------------------------------------- 운영 대시보드 (Phase 10)
# 분석 대시보드(악화 쿼리)와 이원화 — 이쪽은 "파이프라인이 건강한가"를 본다.
# 데이터 원천은 pipeline_run_log(DuckLake, publish/heartbeat가 매 런 발행).
OP_DASHBOARD_NAME = "파이프라인 운영 상태"

OP_LAST_SUCCESS_SQL = """\
select max(dt) as last_success_dt
from pipeline_run_log
where gate_status <> 'FAIL'\
"""

OP_GATE_TODAY_SQL = """\
select
    dt,
    gate_status,
    gate_reconciliation,
    gate_completeness,
    gate_freshness,
    gate_schema_drift,
    run_at
from pipeline_run_log
order by run_at desc
limit 1\
"""

OP_RECENT_RUNS_SQL = """\
select
    dt,
    run_at,
    gate_status,
    round(duration_sec, 1)  as duration_sec,
    offload_rows,
    published_rows
from pipeline_run_log
order by run_at desc
limit 30\
"""


# ---------------------------------------------------------------- 주간 운영 보고 (Phase 16 G6)
# 판정 3종(용량·플랜·백업)의 마지막 마일 — "월요일 보고서"를 마트로 접는다. 발화는 안 하고
# 사람이 pull로 본다(13단계 원칙). 원천은 16단계 마트(DuckLake, publish가 발행).
WEEKLY_DASHBOARD_NAME = "주간 운영 보고"

WEEKLY_REPORT_SQL = """\
select
    instance_id,
    capacity_worst_risk       as capacity_risk,
    min_days_to_threshold     as d_day,
    top_wait_event            as top_wait,
    plan_flips_this_week      as plan_flips,
    plan_regressed_this_week  as plan_regressed,
    backup_gap_days           as backup_gap_days,
    backup_status             as backup_status
from mart_weekly_ops_report
order by instance_id\
"""

BACKUP_RPO_SQL = """\
select
    instance_id,
    as_of_dt,
    last_success_dt,
    gap_days,
    max_gap_days,
    rpo_status
from mart_backup_rpo
where rpo_status <> 'ok'
order by no_successful_backup desc, gap_days desc nulls last\
"""

PLAN_REGRESSION_SQL = """\
select
    instance_id,
    substr(query_id, 1, 20)  as query_id,
    flip_dt,
    prev_plan_hash,
    new_plan_hash,
    before_avg_ms,
    after_avg_ms,
    latency_ratio,
    after_calls,
    verdict
from mart_plan_regression
order by (verdict = 'REGRESSED') desc, latency_ratio desc nulls last\
"""


def ensure_weekly_dashboard(token: str, cards: dict[str, int]) -> int:
    """주간 운영 보고 — 보고표 1장 + 백업 공백(breach/미관측) + 플랜 회귀 목록."""
    for dash in api("GET", "/api/dashboard", token=token):
        if dash["name"] == WEEKLY_DASHBOARD_NAME:
            print(f"[dashboard] 기존 재사용: {WEEKLY_DASHBOARD_NAME} (id={dash['id']})")
            return dash["id"]
    dash = api("POST", "/api/dashboard", token=token, body={
        "name": WEEKLY_DASHBOARD_NAME,
        "description": "용량 D-day·top 대기·플랜 뒤집힘·백업 공백을 한 장으로(16단계). "
                       "판정까지만 계산하고 발화는 안 한다 — 사람이 pull로 본다.",
    })
    dash_id = dash["id"]
    api("PUT", f"/api/dashboard/{dash_id}", token=token, body={
        "dashcards": [
            {"id": -1, "card_id": cards["weekly"], "row": 0, "col": 0,
             "size_x": 24, "size_y": 6},
            {"id": -2, "card_id": cards["backup"], "row": 6, "col": 0,
             "size_x": 12, "size_y": 7},
            {"id": -3, "card_id": cards["plan"], "row": 6, "col": 12,
             "size_x": 12, "size_y": 7},
        ],
    })
    print(f"[dashboard] 생성: {WEEKLY_DASHBOARD_NAME} (id={dash_id})")
    return dash_id


# ---------------------------------------------------------------- 설정 드리프트 (Phase 18)
# "언제 무엇이 바뀌었나" 타임라인 + 그 변경 뒤 성능 회귀가 뒤따랐나(상관). DBTower가 7일류로
# 지우는 이력을 장기로 되살려, 설정 변경을 성능 회귀의 원인 후보로 지목한다.
CONFIG_DASHBOARD_NAME = "설정 드리프트"

CONFIG_CHANGE_SQL = """\
select
    instance_id,
    dt                as change_dt,
    param_name,
    old_value,
    new_value,
    change_kind
from mart_config_change
order by captured_at desc\
"""

CONFIG_IMPACT_SQL = """\
select
    instance_id,
    change_dt,
    param_name,
    new_value,
    plan_flips_after,
    regressed_after,
    correlation
from mart_config_impact
order by regressed_after desc, plan_flips_after desc, change_dt desc\
"""

CONFIG_DAILY_SQL = """\
select
    instance_id,
    dt,
    cycles_collected,
    change_events,
    params_changed
from fct_config_change_daily
order by change_events desc, instance_id\
"""


def ensure_config_dashboard(token: str, cards: dict[str, int]) -> int:
    """설정 드리프트 — 변경 타임라인 + 영향 상관 + 일별 수집/변경."""
    for dash in api("GET", "/api/dashboard", token=token):
        if dash["name"] == CONFIG_DASHBOARD_NAME:
            print(f"[dashboard] 기존 재사용: {CONFIG_DASHBOARD_NAME} (id={dash['id']})")
            return dash["id"]
    dash = api("POST", "/api/dashboard", token=token, body={
        "name": CONFIG_DASHBOARD_NAME,
        "description": "설정 변경 장기 타임라인 + 변경 뒤 플랜 회귀 상관(18단계). "
                       "'누가'는 대상 DB가 안 줘 없다 — 언제·무엇이·그 뒤 무슨 일까지.",
    })
    dash_id = dash["id"]
    api("PUT", f"/api/dashboard/{dash_id}", token=token, body={
        "dashcards": [
            {"id": -1, "card_id": cards["change"], "row": 0, "col": 0,
             "size_x": 12, "size_y": 7},
            {"id": -2, "card_id": cards["impact"], "row": 0, "col": 12,
             "size_x": 12, "size_y": 7},
            {"id": -3, "card_id": cards["daily"], "row": 7, "col": 0,
             "size_x": 24, "size_y": 6},
        ],
    })
    print(f"[dashboard] 생성: {CONFIG_DASHBOARD_NAME} (id={dash_id})")
    return dash_id


def ensure_card(token: str, db_id: int, name: str, sql: str,
                display: str, viz: dict) -> int:
    body = {
        "name": name,
        "display": display,
        "visualization_settings": viz,
        "dataset_query": {
            "type": "native",
            "database": db_id,
            "native": {"query": sql, "template-tags": {"instance_id": INSTANCE_TAG}},
        },
    }
    for card in api("GET", "/api/card", token=token):
        if card["name"] == name:
            # 코드가 진실 — 기존 카드의 SQL/viz를 현재 정의로 갱신(멱등, 스키마 변경 반영).
            api("PUT", f"/api/card/{card['id']}", token=token, body=body)
            print(f"[card] 갱신: {name} (id={card['id']})")
            return card["id"]
    card = api("POST", "/api/card", token=token, body=body)
    print(f"[card] 생성: {name} (id={card['id']})")
    return card["id"]


def ensure_plain_card(token: str, db_id: int, name: str, sql: str,
                      display: str, viz: dict) -> int:
    """파라미터(instance 필터) 없는 카드 — 운영 대시보드용(파이프라인 레벨)."""
    body = {
        "name": name,
        "display": display,
        "visualization_settings": viz,
        "dataset_query": {
            "type": "native",
            "database": db_id,
            "native": {"query": sql, "template-tags": {}},
        },
    }
    for card in api("GET", "/api/card", token=token):
        if card["name"] == name:
            api("PUT", f"/api/card/{card['id']}", token=token, body=body)
            print(f"[card] 갱신: {name} (id={card['id']})")
            return card["id"]
    card = api("POST", "/api/card", token=token, body=body)
    print(f"[card] 생성: {name} (id={card['id']})")
    return card["id"]


def ensure_op_dashboard(token: str, cards: dict[str, int]) -> int:
    """운영 대시보드 — 게이트 상태·최근 런·마지막 성공 dt(pipeline_run_log 원천)."""
    for dash in api("GET", "/api/dashboard", token=token):
        if dash["name"] == OP_DASHBOARD_NAME:
            print(f"[dashboard] 기존 재사용: {OP_DASHBOARD_NAME} (id={dash['id']})")
            return dash["id"]
    dash = api("POST", "/api/dashboard", token=token, body={
        "name": OP_DASHBOARD_NAME,
        "description": "알림(실패)·heartbeat(성공의 부재)에 더한 세 번째 축 — "
                       "'지금 파이프라인이 건강한가'를 한 화면으로. 분석 대시보드와 이원화.",
    })
    dash_id = dash["id"]
    api("PUT", f"/api/dashboard/{dash_id}", token=token, body={
        "dashcards": [
            {"id": -1, "card_id": cards["last_success"], "row": 0, "col": 0,
             "size_x": 6, "size_y": 3},
            {"id": -2, "card_id": cards["gate_today"], "row": 0, "col": 6,
             "size_x": 18, "size_y": 3},
            {"id": -3, "card_id": cards["recent_runs"], "row": 3, "col": 0,
             "size_x": 24, "size_y": 9},
        ],
    })
    print(f"[dashboard] 생성: {OP_DASHBOARD_NAME} (id={dash_id})")
    return dash_id


def ensure_dashboard(token: str, cards: dict[str, int]) -> int:
    for dash in api("GET", "/api/dashboard", token=token):
        if dash["name"] == DASHBOARD_NAME:
            print(f"[dashboard] 기존 재사용: {DASHBOARD_NAME} (id={dash['id']})")
            return dash["id"]

    dash = api("POST", "/api/dashboard", token=token, body={
        "name": DASHBOARD_NAME,
        "description": "0편의 출발 질문에 답하는 화면 — 원천(DBTower)은 7일이면 지우는 "
                       "스냅샷을, lakehouse 마트가 구간 비교로 되살린다.",
    })
    dash_id = dash["id"]
    param_id = "1a2b3c4d"
    mapping = {"parameter_id": param_id,
               "target": ["variable", ["template-tag", "instance_id"]]}
    api("PUT", f"/api/dashboard/{dash_id}", token=token, body={
        "parameters": [{"id": param_id, "name": "Instance", "slug": "instance",
                        "type": "number/=", "sectionId": "number"}],
        "dashcards": [
            {"id": -1, "card_id": cards["scalar"], "row": 0, "col": 0,
             "size_x": 6, "size_y": 4,
             "parameter_mappings": [{**mapping, "card_id": cards["scalar"]}]},
            {"id": -2, "card_id": cards["trend"], "row": 0, "col": 6,
             "size_x": 18, "size_y": 4,
             "parameter_mappings": [{**mapping, "card_id": cards["trend"]}]},
            {"id": -3, "card_id": cards["regression"], "row": 4, "col": 0,
             "size_x": 24, "size_y": 8,
             "parameter_mappings": [{**mapping, "card_id": cards["regression"]}]},
        ],
    })
    print(f"[dashboard] 생성: {DASHBOARD_NAME} (id={dash_id})")
    return dash_id


def main() -> None:
    wait_healthy()
    token = ensure_setup()
    db_id = ensure_database(token)
    cards = {
        "regression": ensure_card(
            token, db_id, "악화 쿼리 랭킹 — first vs last avg latency", REGRESSION_SQL,
            "table", {}),
        "trend": ensure_card(
            token, db_id, "일별 쿼리 호출량 추이", TREND_SQL,
            "line", {"graph.dimensions": ["dt"], "graph.metrics": ["calls"]}),
        "scalar": ensure_card(
            token, db_id, "악화 쿼리 수(구간)", WORST_SQL,
            "scalar", {}),
    }
    dash_id = ensure_dashboard(token, cards)
    print(f"[분석] {MB_URL}/dashboard/{dash_id}")

    # 운영 대시보드(Phase 10) — pipeline_run_log가 있을 때만 얹는다(없으면 건너뜀).
    # 새 테이블(run_log)은 재동기화해야 Metabase 메타데이터에 보인다.
    api("POST", f"/api/database/{db_id}/sync_schema", token=token)
    tables = set()
    for _ in range(30):
        time.sleep(2)
        tables = {t["name"] for t in api("GET", f"/api/database/{db_id}/metadata", token=token)["tables"]}
        if "pipeline_run_log" in tables:
            break
    if "pipeline_run_log" in tables:
        op_cards = {
            "last_success": ensure_plain_card(
                token, db_id, "마지막 성공 dt", OP_LAST_SUCCESS_SQL, "scalar", {}),
            "gate_today": ensure_plain_card(
                token, db_id, "오늘 게이트 상태(최근 런)", OP_GATE_TODAY_SQL, "table", {}),
            "recent_runs": ensure_plain_card(
                token, db_id, "최근 파이프라인 런", OP_RECENT_RUNS_SQL, "table", {}),
        }
        op_dash_id = ensure_op_dashboard(token, op_cards)
        print(f"[운영] {MB_URL}/dashboard/{op_dash_id}")
    else:
        print("[운영] pipeline_run_log 없음 — 운영 대시보드 건너뜀"
              "(extract.run_log로 먼저 발행할 것)")

    # 주간 운영 보고(Phase 16 G6) — 16단계 마트가 발행돼 있을 때만 얹는다.
    api("POST", f"/api/database/{db_id}/sync_schema", token=token)
    tables = set()
    for _ in range(30):
        time.sleep(2)
        tables = {t["name"] for t in api("GET", f"/api/database/{db_id}/metadata", token=token)["tables"]}
        if "mart_weekly_ops_report" in tables:
            break
    if "mart_weekly_ops_report" in tables:
        weekly_cards = {
            "weekly": ensure_plain_card(
                token, db_id, "주간 운영 보고 — 인스턴스별 4계",
                WEEKLY_REPORT_SQL, "table", {}),
            "backup": ensure_plain_card(
                token, db_id, "백업 공백 — breach·미관측",
                BACKUP_RPO_SQL, "table", {}),
            "plan": ensure_plain_card(
                token, db_id, "플랜 회귀 — 뒤집힘 후 지연 판정",
                PLAN_REGRESSION_SQL, "table", {}),
        }
        weekly_dash_id = ensure_weekly_dashboard(token, weekly_cards)
        print(f"[주간] {MB_URL}/dashboard/{weekly_dash_id}")
    else:
        print("[주간] mart_weekly_ops_report 없음 — 주간 보고 건너뜀(publish 먼저)")

    # 설정 드리프트(Phase 18) — mart_config_change가 발행돼 있을 때만.
    api("POST", f"/api/database/{db_id}/sync_schema", token=token)
    tables = set()
    for _ in range(30):
        time.sleep(2)
        tables = {t["name"] for t in api("GET", f"/api/database/{db_id}/metadata", token=token)["tables"]}
        if "mart_config_change" in tables:
            break
    if "mart_config_change" in tables:
        config_cards = {
            "change": ensure_plain_card(
                token, db_id, "설정 변경 타임라인 — 언제 무엇이", CONFIG_CHANGE_SQL, "table", {}),
            "impact": ensure_plain_card(
                token, db_id, "설정 변경 영향 — 뒤이은 플랜 회귀 상관", CONFIG_IMPACT_SQL, "table", {}),
            "daily": ensure_plain_card(
                token, db_id, "일별 수집·변경(무변경 vs 미수집 구분)", CONFIG_DAILY_SQL, "table", {}),
        }
        config_dash_id = ensure_config_dashboard(token, config_cards)
        print(f"[설정] {MB_URL}/dashboard/{config_dash_id}")
    else:
        print("[설정] mart_config_change 없음 — 설정 드리프트 건너뜀(publish 먼저)")
    print(f"\n완료 — ({ADMIN_EMAIL}로 로그인)")


if __name__ == "__main__":
    main()
