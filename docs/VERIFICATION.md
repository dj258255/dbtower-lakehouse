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

## 4. Phase 2 — dbt 변환 (누적 → 일간 델타)

실측 환경 주의: dbt-core 1.11는 python3.14에서 mashumaro 직렬화 오류로 뜨지 않아,
`.venv`를 python3.12로 재구성해 dbt-duckdb 1.10.1을 설치했다(추출 의존성 동일 재설치).
dbt는 `dbt/dbtower_lakehouse`에서 `--profiles-dir .`로 실행(profiles.yml 동거).

### 4-1. 모델 계층

| 모델 | 물질화 | 역할 |
|---|---|---|
| `stg_query_snapshot` | view | raw parquet 직독 + (instance,query,dt,captured_at) SUM 집계로 지문 충돌·중복 계열을 단일 단조 누적 계열로 접음 |
| `fct_query_daily` | table | (instance,query,dt)별 하루 first-vs-last 차분 + `GREATEST(0,…)` 리셋 클램프 → delta_calls/delta_total_time_ms/avg_latency_ms |
| `mart_query_regression` | table | 첫 활동일 대비 마지막 활동일 평균 지연이 악화된 쿼리 랭킹 |

### 4-2. 핵심 함정 — 누적 카운터 + 지문 충돌 (실측)

- raw를 시간순으로 늘어놓으면 `calls`가 302→55→302→56… 처럼 감소가 섞여 가짜 리셋으로 보인다.
  원인: **(instance_id, query_id, captured_at) 중복 12,743키** — 같은 지문(`query_id`)에 둘 이상의
  누적 계열이 얽혀 있다(예: "SHOW REPLICA STATUS"가 calls=302 계열과 55 계열로 동시 존재,
  같은 query_text·다른 id). id는 전역 PK라 계열 식별자가 못 된다.
- **해법**: staging에서 `captured_at`별로 누적값을 SUM. 단조 비감소 계열들의 합도 단조 비감소이므로
  지문 단위 '총 활동'의 누적 계열이 복원된다.
- **방식 선택**: 하루 first-vs-last(양 끝 차분)를 택함 — DBTower `ComparisonService`의
  `Math.max(0, end.calls - start.calls)`와 동일 원리라 교차검증도 된다. 대안인 '인접 스냅샷 양의 델타
  합산'(Prometheus rate 방식)은 SUM-dedup 뒤 쿼리가 스냅샷 간 사라졌다 재등장할 때 유령 증가분을
  과대계상한다(실측 총 delta_calls 22,264,704 vs first-vs-last 3,126,579). 그래서 first-vs-last 채택.
- **리셋 클램프 동작**: SUM-dedup 후에도 순리셋(하루 last < first) 그레인 **219개** 존재 →
  `GREATEST(0,…)`로 0에 클램프. `fct_query_daily.delta_calls` 최솟값 = **0**(음수 0건).

### 4-3. dbt run / test (실측 로그)

```
$ .venv/bin/dbt run --profiles-dir .
  1 of 3 OK view  main.stg_query_snapshot ...... [OK 0.06s]
  2 of 3 OK table main.fct_query_daily ......... [OK 0.27s]
  3 of 3 OK table main.mart_query_regression ... [OK 0.02s]
  Done. PASS=3 WARN=0 ERROR=0 SKIP=0 TOTAL=3

$ .venv/bin/dbt test --profiles-dir .
  Done. PASS=18 WARN=0 ERROR=0 SKIP=0 TOTAL=18
```

테스트 18개 = not_null 14 + relationships 1(fct.instance_id → stg) + 커스텀 singular 3
(`assert_fct_delta_non_negative` = 누적 델타 ≥ 0, `assert_stg_grain_unique`, `assert_fct_grain_unique`).

### 4-4. 마트가 답한 질문 (실측 결과)

`fct_query_daily` 일자별 요약(min delta_calls = 0 = 클램프 정상):

| dt | 그레인 | 총 delta_calls | 총 delta_time |
|---|---|---|---|
| 2026-07-05 | 534 | 624,915 | 141.2 s |
| 2026-07-06 | 448 | 7,458 | 11.4 s |
| 2026-07-07 | 767 | 2,494,206 | 365.9 s |

`mart_query_regression` 21행. "지난 구간보다 느려진 쿼리?" TOP(07-05 → 07-07 평균 지연):

| inst | 쿼리 | first_ms | last_ms | +ms | +% | last_delta_calls |
|---|---|---|---|---|---|---|
| 8(Oracle) | `SELECT sql_id, MAX(SUBSTR(sql_text…` | 25.89 | 64.50 | 38.61 | 149.1 | 686 |
| 4(메타PG 자기쿼리) | `select qs1_0.id,qs1_0.calls,…` | 19.52 | 38.30 | 18.78 | 96.2 | 1,324 |
| 1(MySQL) | ``SELECT `p`.`ID` AS `pid`,…`` | 1.05 | 2.19 | 1.14 | 109.3 | 416 |

inst 4는 DBTower가 자기 메타 PG에 던지는 스냅샷 적재/조회 쿼리 — 파이프라인이 준 부하를 파이프라인이
관측하는 도그푸딩이 데이터로도 드러난다.

### 4-5. 문서/계보 (스크린샷)

- `dbt docs generate` 후 lineage 그래프: raw.query_snapshot(source) → stg_query_snapshot →
  fct_query_daily → mart_query_regression + 커스텀 테스트 3개 분기.
  `docs/images/dbt-lineage.png`.
- dbt run/test 통과 + 마트 질의 실제 출력: `docs/images/dbt-mart-result.png`.

## 5. Phase 3 — 데이터 품질 게이트 (fail-closed)

> 조용히 틀린 데이터는 없는 것보다 나쁘다. raw가 반쪽만 적재됐는데 그 위에 dbt 마트를
> 만들면 "악화 쿼리 랭킹"이 조용히 오답을 낸다. 다운스트림(dbt) 앞에 검문소를 세운다.
> 모듈 `extract/quality.py`, 오케스트레이션 `extract/run_pipeline.py`, DAG는
> `offload → quality_gate → transform`으로 확장.

### 5-1. 세 검문

| 검문 | 판정 규칙 | 실패 시 |
|---|---|---|
| reconciliation | 원천 PG 행수 == parquet 행수(인스턴스별 대조). verify_count 로직 흡수 | FAIL(차단) |
| completeness | 레지스트리(database_instance)의 기대 인스턴스가 그 dt 파티션에 전부 존재 | FAIL(차단) |
| freshness | dt 최신 captured_at이 다음날 00:00에 근접(수집 중단 탐지). 3h 초과 WARN, 12h 초과 FAIL | WARN/FAIL |

fail-closed: 한 dt라도 FAIL이면 `run_pipeline`은 dbt를 아예 호출하지 않고 종료코드 2로 빠진다.
WARN은 통과(경고만). Airflow에선 `quality_gate` 태스크가 예외를 던져 downstream `transform`을 막는다.

### 5-2. 정상 통과 (실측, 3 dt 전부)

```
$ python -m extract.quality 2026-07-05 2026-07-06 2026-07-07
2026-07-05  reconciliation OK  PG=parquet=149,259행 (6인스턴스)
            completeness   OK  기대 6인스턴스 전부 존재
            freshness      OK  최신 23:59:30, 경계까지 0.0h
2026-07-06  ... PG=parquet=79,894행 ... freshness OK 최신 23:58:47
2026-07-07  ... PG=parquet=279,002행 ... freshness OK 최신 23:04:30, 경계까지 0.9h
GATE: PASS — 모든 dt 통과 → 다운스트림 진행 가능   (exit 0)
```

`run_pipeline`으로 이어 붙이면 게이트 통과 후 `dbt run` 실제 실행: PASS=3 WARN=0 ERROR=0.

주의: dt=2026-07-07은 원천 DB의 시계 기준 아직 진행 중인 '오늘'이라(원천 now()가 07-07 23시대)
값이 계속 자란다(268,952 → 269,354 → 279,002). 재적재 직후 그 순간엔 PG=parquet로 맞지만 열린
창이라 다음 순간 또 벌어질 수 있고, freshness가 07-07만 '경계까지 0.9h'로 뜨는 게 그 신호다.
안정 통과 근거는 닫힌 창(07-05·07-06, 149,259·79,894 불변)에 둔다.

### 5-3. 장애 주입 → FAIL로 차단 (실측)

dt=2026-07-06의 `instance_id=3` 파티션(20,158행)을 통째로 삭제(수집 누락 시뮬레이션) 후:

```
$ python -m extract.run_pipeline 2026-07-05 2026-07-06 2026-07-07
2026-07-06  reconciliation FAIL  총 PG 79,894 vs parquet 59,736 — inst 3: PG 20,158 != parquet 0
            completeness   FAIL  기대 6인스턴스 중 누락 [3] (존재 [1, 2, 4, 7, 8])
GATE: BLOCKED — FAIL 파티션 ['2026-07-06'] → dbt 미실행(fail-closed)
=== 2) dbt ===
SKIPPED — 게이트 FAIL. dbt를 실행하지 않는다(fail-closed).   (exit 2)
```

정합·완결성 두 축이 동시에 잡았다. 07-05·07-07은 OK로 통과, 문제 dt만 차단. 시연 후
`python -m extract.offload 2026-07-06`으로 재적재 → 게이트 재통과(PASS) 확인. 리포는 정상 상태.

### 5-4. Airflow — FAIL 시 transform 차단 (실측)

`quality_gate` 태스크에 `retries=0`(품질 FAIL은 결정적이라 재시도 무의미). freshness FAIL 임계를
0.5h로 조여 dt=2026-07-07(경계까지 0.9h)을 강제 FAIL시킨 `airflow dags test` 결과:

```
$ airflow tasks states-for-dag-run snapshot_offload 2026-07-08
offload       success
quality_gate  failed            # 게이트가 raise → 태스크 실패
transform     upstream_failed   # 게이트 실패로 실행되지 않음(반쪽 데이터 위에 마트 안 지음)
```

정상 임계(기본값)로 돌린 `airflow dags test`는 offload·quality_gate·transform 3태스크 전부 success.

- 품질 리포트(정상 OK + 장애주입 FAIL + Airflow 상태): `docs/images/quality-gate.png`.
- Airflow 그래프(offload→quality_gate(빨강 failed)→transform(주황 upstream_failed)): `docs/images/quality-gate-dag.png`.

## 6. Phase 5 — DuckLake 테이블 포맷 (lake → house)

> raw는 파티션 parquet를 통째로 덮어쓴다. 정확·멱등하지만 ACID도 타임트래블도 없어
> 엄밀히는 "lake"다. 그 위에 테이블 포맷 DuckLake를 얹는다. **카탈로그는 PostgreSQL**
> (로컬에 이미 PG가 있어 서비스 추가 0, 단 DBTower 메타 DB와 분리된 `ducklake_catalog`),
> **데이터 파일은 MinIO(S3)**. 모듈 `extract/ducklake_load.py`. 재현: `python -m extract.ducklake_load`.
> 수치는 닫힌 UTC 창(07-05·07-06)만 쓴다 — 07-07은 진행 중인 오늘이라 제외.

### 6-1. ATTACH + 카탈로그가 PG에 생성됨 (실측)

```
$ python -m extract.ducklake_load
[카탈로그 DB] ducklake_catalog @ localhost:15432 (신규 생성) — DBTower 메타 DB(dbtower)와 분리
[ATTACH] ducklake:postgres → DATA_PATH s3://lakehouse/ducklake/  (카탈로그=PG, 데이터=S3)
```

- ATTACH: `ATTACH 'ducklake:postgres:dbname=ducklake_catalog ...' AS lh (DATA_PATH 's3://lakehouse/ducklake/')`.
- PG의 `ducklake_catalog` DB에 카탈로그 테이블 **30개**가 생성됨(`ducklake_snapshot`,
  `ducklake_table`, `ducklake_data_file`, `ducklake_schema` 등). 실측:
  `ducklake_table` = query_snapshot 1건, `ducklake_data_file` = 2건(79,894행·149,259행).
- **DBTower 메타 DB(dbtower) 안의 `ducklake_%` 테이블 = 0** — 원천 메타는 오염되지 않음.

### 6-2. 버전이 쌓인다 — 네 번의 커밋 (실측)

| 커밋 | 동작 | 버전 | 누적 행수 |
|---|---|---|---|
| 1 | CREATE TABLE query_snapshot | 1 | 0 |
| 2 | INSERT dt=2026-07-06 (+79,894) | 2 | 79,894 |
| 3 | INSERT dt=2026-07-05 (+149,259) | 3 | 229,153 |
| 4 | UPDATE id=382457 total_time_ms 0.55→1000.55 | 4 | 229,153(불변) |

`ducklake_snapshots('lh')` 목록(실측): v0 schemas_created → v1 tables_created →
v2/v3 tables_inserted_into → v4 inlined_insert+inlined_delete(단일 행 UPDATE는
DuckLake가 parquet 재작성 대신 카탈로그에 인라인). 벌크 INSERT 2건만 S3에 parquet를 썼다.

### 6-3. 타임트래블 — 과거 버전이 현재와 다름을 실제 조회 (실측)

```
count @ v2 (07-06만 적재 직후)  = 79,894
count @ v3 (07-05까지 적재 직후) = 229,153
count @ v4 (현재)             = 229,153
```

한 행 값의 시점 차이(`AT (VERSION => n)`):

```
total_time_ms @ v3(과거) = 0.55       -- UPDATE 이전
total_time_ms @ v4(현재) = 1000.55    -- UPDATE 이후
```

같은 테이블·같은 쿼리인데 버전 지정만으로 과거 상태(행수·행 값)를 그대로 되살렸다.
raw 덮어쓰기로는 불가능했던 것 — 이 지점이 lake가 house가 되는 곳이다.

### 6-4. 원자성 — BEGIN … ROLLBACK (실측)

```
트랜잭션 전 count       = 229,153
DELETE 07-05 후(txn 내) = 79,894
ROLLBACK 후 count       = 229,153   (원상복구)
스냅샷 수 5 → 5  (롤백은 버전을 남기지 않음)
```

트랜잭션 안에서 149,259행을 지워도 ROLLBACK 하면 흔적 없이 되돌아가고, 스냅샷(버전)도
남기지 않는다. 부분 반영이 없다.

### 6-5. 데이터=S3 / 카탈로그=PG 분리 (실측)

```
s3://lakehouse/ducklake/main/query_snapshot/*.parquet
  ducklake-...9163.parquet   810,322 bytes  (79,894행)
  ducklake-...f045.parquet 1,384,993 bytes  (149,259행)
```

카탈로그(메타데이터)는 PG, 실제 컬럼나 데이터는 S3. 스토리지/컴퓨트 분리가 테이블
포맷에서도 유지된다. 증거 이미지: `docs/images/ducklake-timetravel.png`(실제 실행 출력).

## 7. 잔여 (정직)

- 원천이 라이브라 "완전히 닫힌 최신 구간"은 하루 뒤에야 안정. 07-07 값은 시점 의존.
- 지문 충돌은 SUM 집계로 접었지만, 이는 서로 다른 물리 쿼리를 하나로 합치는 근사다. id로 계열을
  완벽히 분리하진 못한다(id는 스냅샷마다 새로 발번). 지문 단위 '총 활동'까지가 정직한 한계.
- first-vs-last는 하루 중 리셋 이후 재상승분을 일부 잃는다(순리셋 219그레인). DBTower 정식 로직과의
  정합을 우선해 감수했고, 잃는 양은 클램프된 그레인 수로 계량해 두었다.
- 품질 게이트는 규칙 기반까지다(정합·완결성·freshness). 통계적 이상 자동 감지·알림 발화(웹훅)는 범위 밖 — 5절.
- freshness는 dt 파티션 전체의 최신 captured_at으로 판정한다. 일부 인스턴스만 일찍 끊긴 경우
  다른 인스턴스가 경계까지 수집했으면 dt-level로는 OK가 될 수 있다(인스턴스별 freshness는 향후).
- 3일치(07-05~07)만 적재돼 회귀 비교는 사실상 이틀 간. '지난달' 규모 추세는 적재 누적 후.
