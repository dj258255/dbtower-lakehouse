# dbtower-lakehouse — 버려지는 관측 데이터의 장기 분석 파이프라인

> DBTower가 5기종(MySQL·PostgreSQL·SQL Server·Oracle·MongoDB)에서 수집한 쿼리 스냅샷은
> 메타 DB 포화 방지를 위해 **7일 뒤 삭제된다**(AWS Performance Insights 기본 보존 선례).
> 그런데 "이번 달 vs 지난달 회귀 추세", "분기 용량 계획", "기종별 성능 비교" 같은 질문은
> 장기 이력이 있어야 답할 수 있다. 이 프로젝트는 **만료 직전의 스냅샷을 컬럼나 저장소로
> 내려(ELT) 장기 분석을 가능하게 하는 데이터 파이프라인**이다.

## 한 줄 정체성

**운영계(DBTower, 관제)와 분석계(lakehouse, 장기 이력)를 분리**하고, 그 사이를
일 배치 파이프라인으로 잇는다 — 실무에서 OLTP와 DW를 분리하는 그 구조의 축소판.

![dbtower-lakehouse 파이프라인 — query_snapshot을 Airflow로 추출·적재하고 dbt로 집계해 DuckDB로 질의, 사이에 데이터 품질 게이트](docs/architecture.svg)

## 스택 (전부 로컬에서 e2e 재현 가능)

| 층 | 도구 | 선택 이유 |
|---|---|---|
| 오케스트레이션 | **Apache Airflow** (docker compose, LocalExecutor) | 업계 표준. 레거시 Oozie와 개념(DAG·스케줄) 동일 |
| 저장 | **MinIO(S3 호환) + Parquet** | DBTower 데모 스택에 이미 있음(재사용). 스토리지/컴퓨트 분리 = lakehouse의 정의 |
| 변환 | **dbt-core + dbt-duckdb** | SQL 기반 변환·테스트·문서화. Hive 가공의 현대판 |
| 테이블 포맷 | **DuckLake** (카탈로그=PostgreSQL, 데이터=parquet) | ACID·타임트래블·스키마 진화 = "lake"를 "lakehouse"로. 이미 PG를 써서 카탈로그 DB 추가 0 (Iceberg는 REST 카탈로그 서버 필요라 로컬엔 과함) |
| 쿼리 엔진 | **DuckDB** | S3 parquet 직독 + DuckLake first-class 지원. 무료·로컬·빠름 |
| 품질 | **dbt tests (+ 필요 시 Great Expectations)** | freshness·중복·스키마 검증, 실패 시 웹훅 |
| 언어 | **Python 3.12** | DAG·추출 스크립트 |

## 원칙 (DBTower에서 계승)

1. **정직한 필요에서 시작한다** — 도구부터 나열하지 않는다. 모든 단계는 "왜 필요한가"가 먼저다.
   (그래서 Kafka·Spark는 초기 범위에서 뺐다 — 일 수만 행 배치에 스트리밍·분산은 과잉. 잔여로 명시)
2. **실측 필수** — 모든 단계는 로컬 e2e 라이브 실측 + 스크린샷 + `docs/VERIFICATION.md` 절 번호 기록.
3. **부하 원칙** — 추출이 운영계(메타 PG)의 부하가 되면 안 된다. 시간창·LIMIT·읽기 전용.
4. **못 하는 것은 못 한다고** — 근사·표본·미지원은 표기한다.
5. **블로그** — 단계마다 개선 아크(한계 인지 → 판단 → 개선 → 실측 → 잔여)로 기록.

## 관련 저장소

- 데이터 원천: [DBTower](https://github.com/dj258255/dbtower) — 이 파이프라인이 없으면 그 관측 데이터는 7일 뒤 소멸한다.

## 로드맵

[docs/ROADMAP.md](docs/ROADMAP.md) — Phase 0~5 상세(구현 방법·함정·검증 기준·산출물).
