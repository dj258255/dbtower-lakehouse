# lakehouse Airflow 이미지 — Phase 6 운영 경화.
#
# 이전(Phase 0~5)에는 _PIP_ADDITIONAL_REQUIREMENTS로 컨테이너 기동 때마다 pip을
# 돌렸다(재기동마다 재설치·비재현적)  그리고 dbt가 컨테이너에 아예 없어서 transform이
# 호스트 수동 실행에 의존했다 — 오케스트레이션의 최대 구멍. 이 이미지가 둘 다 닫는다.
#
# dbt는 Airflow와 **별도 venv(/opt/dbt-venv)** 에 격리한다. dbt-core와 Airflow는
# 공유 의존성(jinja2, click, protobuf 등)의 버전 요구가 자주 충돌하는 것으로 널리
# 알려져 있어, 같은 site-packages에 섞으면 어느 한쪽 업그레이드가 다른 쪽을 조용히
# 깨뜨린다. venv 분리가 관례다(Cosmos·MWAA 문서도 같은 이유로 분리 실행을 권장).
#
# 빌드: docker compose build  (compose가 이 Dockerfile을 참조한다)

FROM apache/airflow:2.10.4-python3.12

# 1) 추출·게이트·DuckLake 유지보수 런타임 의존성 — Airflow venv에 직접 얹는다.
#    duckdb는 ducklake 확장을 지원하는 1.3+ 필요(호스트 실측과 동일한 1.5.4로 고정).
RUN pip install --no-cache-dir \
    pyarrow==18.1.0 \
    psycopg2-binary==2.9.10 \
    boto3==1.35.90 \
    duckdb==1.5.4

# 2) dbt는 분리 venv에. Airflow 의존성과 절대 섞지 않는다.
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir \
        dbt-duckdb==1.10.1 \
        duckdb==1.5.4 \
    && chown -R airflow:0 /opt/dbt-venv
USER airflow

# 3) 코드를 이미지에 굽는다 — standalone 어플라이언스(ROADMAP 11단계)의 재현성.
#    dev용 docker-compose.yml 은 여전히 ./dags 등을 bind-mount로 덮으므로 개발 경로는 불변.
#    standalone 은 bind-mount가 없어 이 baked 코드를 쓴다. pip 레이어 뒤라 캐시 보존.
COPY --chown=airflow:0 dags/ /opt/airflow/dags/
COPY --chown=airflow:0 extract/ /opt/airflow/extract/
COPY --chown=airflow:0 dbt/ /opt/airflow/dbt/
