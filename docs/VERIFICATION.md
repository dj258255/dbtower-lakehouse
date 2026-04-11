# VERIFICATION — 라이브 실측 기록

> 모든 수치는 실제로 돌린 결과다. 지어낸 것 없음. 재현 명령·원천 대조를 함께 남긴다.
> 실측 환경: macOS, Docker 27.4, Python 3.14(호스트 venv) / Airflow 이미지 python3.12.
> 실측 시각 기준 원천(DBTower)이 라이브로 수집 중이라, "닫힌 UTC 구간"으로 검증한다.

## 1. Phase 0 — 스캐폴드 기동

- **원천 확인**: `docker ps`로 `dbtower-postgres`(15432)·`dbtower-minio`(19000) 재사용. MinIO health 200.
- **query_snapshot 존재**: 647,327행, instance 6개(1·2·3·4·7·8), captured_at 2026-07-03~07-07(UTC).
- **Airflow 기동(LocalExecutor)**: `docker compose up -d` →
  `airflow-postgres`(메타, 격리)·`airflow-scheduler`·`airflow-webserver`(:8080) 정상.
  init 컨테이너 exit 0. `airflow dags list-import-errors` → **No data found**(임포트 에러 0).
- **DAG 노출**: `airflow dags list` →
  `snapshot_offload | /opt/airflow/dags/snapshot_offload.py | airflow | True`.
- 스크린샷 자리: Airflow UI 첫 화면(DAG 목록), MinIO 콘솔 로그인(19001).

## 2. Phase 1 — Extract & Load e2e

### 2-1. 라이브(진행 중) 구간 dt=2026-07-07

호스트 venv에서 `python -m extract.offload 2026-07-07` 실행.

| instance | 적재 행수 |
|---|---|
| 1 | 38,745 |
| 2 | 66,041 |
| 3 | 40,499 |
| 4 | 73,080 |
| 7 | 2,853 |
| 8 | 47,734 |
| **합계** | **268,952** |

- **원천 대조**(같은 시점 PG count, 동일 UTC 창): **268,952** — 정확히 일치.
- **DuckDB 대조**: `read_parquet('s3://…/dt=2026-07-07/instance_id=*/*.parquet', hive_partitioning=1)`
  count = **268,952** — 원천·적재·조회 3자 일치.
- 스키마 검증(DuckDB DESCRIBE): id BIGINT, instance_id BIGINT, captured_at TIMESTAMP,
  query_id VARCHAR, query_text VARCHAR, calls BIGINT, total_time_ms DOUBLE, rows_examined BIGINT,
  dt DATE(파티션). 선언 스키마와 일치.
- 주의: 07-07은 실측 시점(UTC 07-07 21:5x)에 원천이 라이브 수집 중이라 값이 계속 커진다.
  재실행 시 268,952 → 269,354로 증가한 것은 **원천의 실제 신규 데이터**이지 중복이 아니다
  (오브젝트는 인스턴스당 여전히 1개). 멱등성은 닫힌 구간(2-2)으로 검증한다.

### 2-2. 멱등성 — 닫힌 구간 dt=2026-07-06 (2회 실행)

| 항목 | 값 |
|---|---|
| 원천 PG count | 79,894 |
| offload run A 합계 | 79,894 |
| offload run B 합계(재실행) | **79,894** (불변) |
| 파티션 오브젝트 수 | **6** (인스턴스당 1, 누적 안 됨) |
| DuckDB count | 79,894 |

→ 같은 날짜를 두 번 돌려도 행수·오브젝트 수 불변. whole-partition overwrite 멱등성 확인.

### 2-3. Airflow 스케줄러 e2e — `airflow dags test`

컨테이너 안에서 `airflow dags test snapshot_offload 2026-07-06` 실행:

- 논리 실행일 2026-07-06 → `data_interval_start` = 2026-07-05 → 태스크가 **dt=2026-07-05**를 처리.
  (start_date를 @daily 자정에 정렬해 data interval이 어긋나지 않음을 실증 — "어제" 의미 정확.)
- 결과: `state=success`, 반환값 `total_rows=149,259`.
- **원천 대조** dt=2026-07-05 PG count = **149,259**, **DuckDB** count = **149,259** — 일치.
- 즉 스케줄러가 부른 태스크가 원천→parquet→조회까지 정확히 흘렀다.

스크린샷 자리: Airflow 성공 런(그리드/그래프 뷰), MinIO 콘솔의 파티션 트리, DuckDB count 터미널.

## 3. 부하 원칙 확인

- 원천 세션을 `readonly=True`로 열어 쓰기 자체를 세션 레벨에서 차단.
- instance별 등치 질의(`WHERE instance_id=? AND captured_at>=? AND captured_at<?`)로
  `idx_snapshot_instance_time(instance_id, captured_at)` 선두 컬럼을 탄다(풀스캔 회피).
- 서버커서(named cursor, itersize 50,000)로 결과 전체를 메모리에 올리지 않는다.
- 운영 대상 DB(mysql/oracle 등)에는 접근하지 않음 — 관측 전용 메타 PG에서만 추출.

## 4. 잔여 (정직)

- 원천이 라이브라 "완전히 닫힌 최신 구간"은 하루 뒤에야 안정. 07-07 값은 시점 의존.
- raw는 아직 질문에 답 못 함(누적값). 일간 델타·회귀 랭킹은 Phase 2(dbt).
- 품질 게이트(빈 파티션·freshness) 없음 — Phase 3.
- 스케줄러 상시 구동은 로컬 리소스 절약 위해 검증 후 정지 가능(DAG 구조·e2e 실행은 위에서 확인).
