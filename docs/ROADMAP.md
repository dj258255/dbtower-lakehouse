# dbtower-lakehouse 로드맵 — 상황 주도 개선 아크

> 이 문서는 기능 나열이 아니라 **"어떤 상황에서 무엇이 깨지고, 그래서 무엇을 만드는가"**로 쓴다.
> 각 Phase = (상황 가정) → (그 상황에서 드러나는 한계) → (판단) → (개선) → (라이브 실측·스크린샷) → (정직한 잔여).
> DBTower 시리즈에서 검증된 서사 원칙 그대로. 웹서칭으로 실전 함정을 검증한 뒤 적었다(각 근거 URL 명시).

---

## 0. 이 프로젝트가 존재하는 이유 — 한 상황에서 출발

**상황 가정**: DBTower를 3개월 운영했다. "지난달보다 이번 달에 느려진 쿼리 있어?"라고 물었다.
답할 수 없었다 — DBTower의 스냅샷은 **7일 뒤 삭제**되기 때문이다(메타 DB 포화 방지,
`SnapshotRetentionJob` + `retention-days: 7`, AWS PI 무료 티어 7일 선례를 따름).

**한계**: 운영 관제(DBTower)는 "지금~최근"에 최적화됐고, 그래야 한다(관제 DB가 무한 성장하면 안 됨).
하지만 "장기 추세·용량 계획·분기 비교"라는 질문은 그 설계로는 구조적으로 못 답한다.

**판단**: 운영계(OLTP성 관제)와 분석계(장기 이력)를 **분리**한다 — 실무에서 프로덕션 DB와 DW를
분리하는 그 원칙. 버려지기 직전의 스냅샷을 컬럼나 저장소로 내려(ELT) 분석계를 만든다.

**이 프로젝트 = 버려지는 관측 데이터의 두 번째 삶.**

### 전제 사실 (2026-07-08 웹·코드 검증 — 지어낸 것 없음)

| 전제 | 근거 | 상태 |
|---|---|---|
| DBTower 스냅샷 7일 후 삭제 | `application.yml` retention-days: 7 + `SnapshotRetentionJob.java` | 코드 확인 |
| 7일 = AWS PI 무료 티어 선례 | AWS 공식 문서(무료 7일, 이상은 유료) | 웹 확인 |
| MinIO(S3 호환) 데모 스택에 이미 존재 | `docker-compose.yml` dbtower-minio | 코드 확인 |
| DBTower 시각 UTC 고정(파티션 경계 안정 전제) | hibernate.jdbc.time_zone=UTC + TimeZone.setDefault(UTC) | 코드 확인(하드닝 아크) |
| Airflow+dbt+DuckDB+MinIO는 2025 실제 lakehouse 표준 조합 | 다수 튜토리얼·GitHub 프로젝트 실존 | 웹 확인 |
| DuckLake는 카탈로그를 PG에 두는 실존 테이블 포맷 | ducklake.select + dbt-duckdb 1.9.6+ is_ducklake | 웹 확인 |

---

## 공통 가드레일 (전 Phase — 위반 시 실패)

1. **분석이 운영의 부하가 되면 안 된다** — DBTower의 A9 원칙을 계승. 메타 PG 추출은 읽기 전용 +
   시간창(어제) + 배치 크기 제한. **운영 대상 DB에서 직접 뽑지 않는다**(부하). 관측 전용인 메타 PG에서만.
   업계 정석(CDC/read replica)과 같은 이유 — [Fivetran/Airbyte 추출 best practice](https://www.automq.com/blog/fivetran-vs-airbyte-elt-tools-comprehensive-comparison).
2. **멱등성** — 같은 논리 날짜를 몇 번 돌려도 결과 동일(파티션 덮어쓰기/upsert). backfill 안전.
   [Airflow 멱등성 원칙](https://tomasfarias.dev/articles/writing-idempotent-dbt-tasks-for-airflow/).
3. **정직 표기** — 지연 도착·중복·품질 실패·근사는 감추지 않고 메트릭/노트로.
4. **실측 필수** — Phase마다 로컬 e2e + 스크린샷(docs/images/) + docs/VERIFICATION.md 절 기록.
5. **도메인 섞기 금지** — 다른 프로젝트(pay 등)의 성과를 이 프로젝트 커버리지로 계산하지 않는다.
   pay의 Kafka는 결제 이벤트지 이 파이프라인이 아니다.
6. **범위 정직** — Kafka 스트리밍·Spark·클라우드 DW는 현 규모(일 수만 행)에 과잉. 잔여에 "언제 필요한가"와 명시.
7. **관례** — 커밋 한국어, 이모지 금지, 블로그 개선 아크.

---

## Phase 0 — 계약 먼저 (스캐폴드)

**상황**: 코드부터 짜고 싶다. 하지만 "무엇을 몇 시에 어떤 형태로 옮기는지" 계약 없이 DAG를 짜면,
파티션 규칙·스키마가 나중에 흔들려 backfill이 깨진다.

**한계 인지**: 파이프라인의 버그는 대부분 "계약 불명확"에서 온다 — dt 경계가 UTC냐 KST냐,
파티션 키가 뭐냐, 스키마가 진화하면 옛 파일은 어떻게 읽냐.

**개선(구현)**:
- 저장소 구조: `dags/` `dbt/` `extract/`(Python) `docker-compose.yml`(Airflow LocalExecutor +
  기존 dbtower-minio 재사용, external network 연결) `docs/`.
- 데이터 계약 문서(`docs/CONTRACT.md`): 원천 스키마(query_snapshot 컬럼), 파티셔닝
  (`dt=YYYY-MM-DD/instance_id=N/`), 포맷(parquet+zstd), 워터마크 전략.
- **함정(웹 검증)**: Airflow docker compose 기본은 CeleryExecutor(무거움) → LocalExecutor로.
  `start_date`와 `@daily` 정렬 안 하면 첫 인터벌이 어긋난다 —
  [data interval 정렬](https://towardsdatascience.com/airflow-data-intervals-a-deep-dive-15d0ccfb0661/).
  `catchup=False`로 시작(무의도 대량 백필 방지).

**실측**: `airflow dags list`에 DAG 노출, MinIO 헬스, Airflow UI 첫 화면 스크린샷.
**잔여**: 아직 데이터 안 흐름 — Phase 1에서.
**산출물**: VERIFICATION 1절 · 블로그 0편 "버려지는 데이터에서 시작하는 파이프라인 — 계약 먼저".

---

## Phase 1 — Extract & Load (EL)

**상황**: 어제 쌓인 스냅샷이 오늘 자정이 지나면 6일 남았다. 6일 뒤 삭제되기 전에 안전하게 내려야 한다.

**한계 인지**:
- 추출 쿼리가 메타 PG에 부하를 주면 안 된다(관제탑을 느리게 하는 자기모순).
- **DBTower의 `idx_snapshot_instance_time`은 `(instance_id, captured_at)` 순서** — `captured_at`
  단독 조건은 선두 컬럼이 아니라 인덱스를 못 탈 수 있다(하드닝 감사에서 이미 지적된 실제 제약).

**판단**: 시간창(어제)+instance별 루프로 인덱스 선두(instance_id)를 타게 하거나, 부하가 확인되면
그때만 원천 인덱스 보강 검토(최후 수단). 절대 원천을 함부로 바꾸지 않는다.

**개선(구현)**:
- 일 배치 DAG `snapshot_offload`: 매일 UTC 새벽, `captured_at`이 어제(data_interval)인 행을
  메타 PG SELECT → pyarrow parquet → MinIO `s3://lakehouse/raw/query_snapshot/dt=.../instance_id=.../`.
- **멱등성**: 파티션 통째 덮어쓰기(임시 경로 쓰고 성공 시 교체). 같은 날짜 2회 실행해도 행수 불변.
- **함정(웹 검증)**: backfill은 수백 태스크를 동시 실행해 스케줄러·워커·downstream을 짓누를 수 있다 —
  [backfill 자원 함정](https://risingwave.com/blog/avoiding-airflow-backfill-pitfalls-expert-advice/). 동시성 상한.
  parquet 스키마를 명시 선언(조용한 타입 추론 변화 차단).

**실측(라이브)**:
- e2e: DBTower 하루 돌린 실제 스냅샷 → DAG 트리거 → MinIO에 parquet →
  DuckDB `SELECT count(*) FROM read_parquet('s3://...')`가 원천 행수와 일치.
- 멱등: 같은 날짜 2회 → 행수 불변(중복 0).
- **도그푸딩**: 추출 중 메타 PG 부하를 DBTower 자신으로 관측(파이프라인이 준 부하를 관제탑이 감시하는 순환).
**잔여**: raw는 질문에 못 답함 — Phase 2.
**산출물**: VERIFICATION 2절 · 스크린샷(Airflow 성공 런·MinIO·DuckDB 카운트) · 블로그 1편.

---

## Phase 2 — Transform (dbt)

**상황**: "지난 30일 가장 나빠진 쿼리 TOP 10"을 물었다. raw parquet엔 누적값만 있어 답이 안 나온다.

**한계 인지**: **query_snapshot의 calls/total_time_ms가 누적값인지 구간값인지 코드로 먼저 확인해야 한다**
(단정 금지 — DBTower 시점 비교 로직을 봐야 함. 누적이면 일간 델타 계산이 핵심 변환이 된다).

**개선(구현, dbt-duckdb)**:
- staging: `stg_query_snapshot` — 타입 정리, dedup(instance+query+captured_at).
- marts:
  - `fct_query_daily` — (dt,instance,query_id)별 일간 델타(누적일 경우) — incremental 모델.
  - `mart_regression_trend` — 주간 이동평균 대비 악화율 랭킹.
  - `mart_engine_compare` — 기종별 레이턴시 분포.
- **함정**: 누적 카운터 리셋(대상 재기동 시 감소) → 음수 델타는 리셋으로 간주·클램프
  (DBTower OpsAlert의 음수 델타 클램프 규칙 재사용 — 이미 검증된 로직). late-arriving 파티션.

**실측**: 수동 파티션에 대한 델타 계산 단위 검증(dbt test) + 실제 30일 데이터 mart 결과가
DBTower 화면의 시점 비교와 일치하는 교차 검증 1건(스크린샷 나란히).
**잔여**: 조용히 틀린 데이터 위험 — Phase 3.
**산출물**: VERIFICATION 3절 · 블로그 2편 "누적 스냅샷을 일간 델타로".

---

## Phase 2.5 — 거버넌스·카탈로그 (dbt 네이티브)

**상황**: 모델이 10개를 넘었다. "이 마트 뭐냐, 컬럼 의미가 뭐냐, 누가 쓰냐, 믿어도 되냐"를
사람이 기억 못 한다. 스키마를 몰래 바꾸면 다운스트림이 조용히 깨진다.

**한계 인지**: 데이터가 자산이 되려면 문서·계약·소유권이 있어야 한다. **단, 이건 Atlan 같은
카탈로그 "제품"을 만드는 게 아니라 dbt 네이티브 기능을 제대로 쓰는 것**(과장 금지).

**개선(구현, 새 도구 0 — 전부 dbt 내장, 웹 검증)**:
- 데이터 사전: 모든 모델·컬럼 description → `dbt docs generate`로 카탈로그 사이트(계보 자동 추론).
- 데이터 계약: marts에 `contract: enforced: true` + 컬럼 타입·제약 선언 → 스키마 몰래 바뀌면 빌드 실패.
  [Model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts).
- 접근 경계: `access: public`(마트) / `private`(스테이징), `group`으로 소유권.
  [Model governance](https://docs.getdbt.com/docs/mesh/govern/about-model-governance).
- exposures: "이 대시보드는 mart_regression 의존" 선언 → 영향 분석.

**실측**: `dbt docs serve` 계보 그래프 스크린샷(스냅샷→마트 전체 흐름). 고의로 마트 컬럼 타입 변경 →
계약 위반 빌드 실패 스크린샷. 카탈로그 UI 컬럼 설명 스크린샷.
**잔여**: 컬럼 레벨 계보·PII 태깅은 dbt Enterprise 기능(범위 밖) — 문서 계보까지만.
**산출물**: VERIFICATION 절 · 블로그 "데이터를 자산으로 — dbt 거버넌스".

---

## Phase 3 — 데이터 품질 게이트

**상황**: 어느 날 특정 인스턴스의 어제 파티션이 비었다(수집 장애). 그 위에 만든 "악화 쿼리 랭킹"은
조용히 틀린 답을 냈다. 아무도 몰랐다.

**한계 인지**: 조용히 틀린 데이터는 없는 것보다 나쁘다(DBTower "못 하는 것은 못 한다고" 원칙의 데이터판).

**개선(구현)**:
- dbt tests(not_null·unique·accepted_range) + source freshness(어제 파티션 존재) → 실패 시 Airflow
  콜백으로 웹훅(DBTower와 같은 Discord 채널 재사용 가능).
- error vs warn 구분: 지연 도착=warn, 스키마 위반·freshness 실패=error(파이프라인 차단).
- GE(Great Expectations)는 dbt test로 부족할 때만 도입(도구 겹침 방지, 판단 근거 기록).

**실측**: 고의로 깨진 파티션 주입 → 게이트가 잡고 알림 발화 + 정상 파티션 통과. 스크린샷.
**잔여**: 이상 자동 감지(통계적)는 범위 밖 — 규칙 기반까지.
**산출물**: VERIFICATION 4절 · 블로그 3편 "실패해야 하는 파이프라인".

---

## Phase 4 — Serve (질문에 답하기)

**상황**: 이 모든 게 결국 "지난 30일 가장 나빠진 쿼리는?"에 답하려는 거였다. 답을 눈에 보이게.

**개선(구현)**: DuckDB 애드혹 + 경량 대시보드 1장(Evidence 또는 Metabase 하나만 — 판단 기록):
"인스턴스별 30일 악화 쿼리 TOP 10 + 추세선".

**실측(핵심 대비)**: **DBTower(7일 시야)로는 "데이터 없음", lakehouse(30일 시야)로는 답이 나오는 것을
나란히 스크린샷** — 이 대비가 프로젝트 전체의 존재 증명.
**산출물**: VERIFICATION 5절 · 블로그 4편.

> 실행 기록: 이 Serve 단계는 품질 게이트(블로그 4편)·DuckLake(5편)·운영 경화(6편)를
> 먼저 닫은 뒤 **Phase 7(아래)에서 Metabase 대시보드로 구현**했다 — 반쪽 데이터 위에
> 화면부터 얹지 않으려는 순서 조정.

---

## Phase 5 — 테이블 포맷(DuckLake): "lake"를 "lakehouse"로

**상황**: mart 적재가 도중일 때 대시보드를 열었더니 반쪽 데이터가 보였다. "지난주 기준으로 다시 계산해줘"는
아예 불가능했다. 컬럼 하나 추가했더니 옛 파티션 읽기가 깨졌다.

**한계 인지**: Phase 1~4의 "parquet 파티션 덮어쓰기"는 lake다. lakehouse의 정의(개방 포맷 위에
**ACID·타임트래블·스키마 진화**)를 채우려면 테이블 포맷이 있어야 한다. 없으면 lakehouse라 부르는 게 과장.

**판단 — DuckLake (Iceberg 대신, 웹 검증)**:
- Iceberg 쓰기는 DuckDB에서 **REST 카탈로그 서버 필수**(v1.4+, path 기반은 읽기 전용) — 로컬 단일노드에
  서비스 하나가 더 는다. [DuckDB Iceberg writes](https://duckdb.org/docs/current/core_extensions/iceberg/writing).
- DuckLake는 카탈로그를 **PostgreSQL에 SQL로** 두고 데이터는 parquet — **이미 PG를 써서 서비스 추가 0**.
  [ducklake.select](https://ducklake.select/).
- 표준은 Iceberg지만 로컬 단일노드에는 DuckLake가 구조적으로 맞다. 타임트래블·스키마 진화 개념은
  동일하므로 Iceberg 전환은 어댑터 문제(미지원이 아니라 규모 부적합).

**개선(구현)**:
- DuckLake 카탈로그용 PG(DBTower 메타 PG와 **물리 분리**) → `ATTACH 'ducklake:postgres:...'`.
- marts를 DuckLake 테이블로(dbt-duckdb `is_ducklake: true`, `partitioned_by`).
- 타임트래블 질의로 "과거 버전 기준 재계산" 시연.
- **함정**: 카탈로그 PG 분리(오염·권한). 버전 폭증 방지(스냅샷 만료). 스키마 진화 시 기존 파티션 호환.

**실측(라이브)**: (a) 적재 도중 조회해도 이전 버전 온전(ACID), (b) 어제 버전 타임트래블 성공,
(c) 컬럼 추가 후 과거 데이터 조회 정상. 각 스크린샷.
**산출물**: VERIFICATION 6절 · 블로그 5편 "lake에서 house로".

---

## Phase 6 — 운영 경화: 실패해도 아무도 모르는 파이프라인은 미완성

**상황**: Phase 3의 fail-closed 게이트가 반쪽 데이터를 잘 막았다. 그런데 막았다는 사실을
**아무도 모른다** — 알림이 없어서 조용히 멈춘 채로 발견될 때까지 마트가 낡아간다. 게다가
transform 태스크는 컨테이너에 dbt가 없어 실제 빌드가 호스트 수동 실행이었다 — DAG 그래프는
3단계인데 마지막 단계가 사람 손이라면 그건 반쪽 오케스트레이션이다. DuckLake는 커밋마다
버전을 쌓는데 만료가 없어 방치 시 카탈로그·S3가 단조 증가한다.

**한계 인지**: 차단은 시작이고 통보가 완성이다. 재현 안 되는 컨테이너(기동마다 pip)와
문서 없는 backfill은 "만든 사람만 돌릴 수 있는 파이프라인"이다.

**개선(구현)**:
- 실패 알림: `extract/alerts.py` — default_args `on_failure_callback` → `ALERT_WEBHOOK_URL`로
  JSON POST(태스크·실행일·로그 URL·에러 요약). 알림 실패는 삼킨다(파이프라인 무영향).
  SLA 콜백은 폐기 경로라 배제(3.0 제거).
- retry 정책: retries=3 + 지수 백오프(2→4→8분). 단 quality_gate는 retries=0 유지(결정적 실패).
  전 PG DSN connect_timeout=5(무한 대기 차단 — 실제 원천 다운 사고에서 배움).
- 컨테이너에 dbt: `Dockerfile` — 분리 venv(/opt/dbt-venv)에 dbt-duckdb(Airflow 의존성과 격리),
  `_PIP_ADDITIONAL_REQUIREMENTS` 폐기. transform이 컨테이너 안에서 dbt run+test 실행.
- DuckLake 유지보수: `ducklake_maintenance` DAG(@weekly) — 공식 권장 번들 CHECKPOINT
  (만료+플러시+컴팩션, 손 순서 이슈 회피) + 삭제 예약 파일 정리. 보존 7일(원천과 대칭).
- `docs/RUNBOOK.md`: 실패 대응(알림→로그→재적재)·backfill 레시피(-s/-e 날짜 산수 실측
  포함)·유지보수 주기.

**실측**: 3태스크 전부 컨테이너 안 success(dbt PASS=3/test PASS=18) · 강제 FAIL 시 webhook
실수신 · CHECKPOINT 전후 스냅샷 11→2, S3 7→3오브젝트(행수 불변) · backfill 후 행수 불변.
`docs/VERIFICATION.md` 7절.

**잔여**: 증분 모델·알림 라우팅·적응형 유지보수 주기 — VERIFICATION 10절 참조.

---

## Phase 7 — 대시보드: 마트는 있는데 소비자가 없다

**상황**: 파이프라인은 매일 마트를 굽는데, 그 답을 보려면 여전히 DuckDB 셸에 SQL을
쳐야 한다. 0편의 질문("지난 구간보다 느려진 쿼리 있어?")을 던진 사람은 SQL을 치는
사람이 아니다. 마트에 소비자가 없으면 파이프라인은 출구 없는 공장이다.

**판단 — Metabase (셀프서비스 BI)**:
- 정적 리포트(Evidence류·노트북 내보내기)는 "만든 질문"에만 답한다. 필터로 파고드는
  탐색(인스턴스별·기간별)은 BI 서버가 맞다.
- Metabase는 MotherDuck이 유지하는 DuckDB 커뮤니티 드라이버가 있어 서빙 DB 추가 없이
  DuckDB/DuckLake를 직접 읽고, 초기 설정→커넥션→질문→대시보드가 전부 REST API라
  재현을 스크립트로 못박을 수 있다(`scripts/metabase_bootstrap.py`).

**개선(구현)**:
- compose에 `metabase` 서비스(:13001). 공식 이미지가 Alpine이라 DuckDB JDBC 네이티브
  라이브러리가 안 떠서(**함정 1**, glibc) Debian 기반 커스텀 이미지(`metabase/Dockerfile`)에
  드라이버를 굽는다. 드라이버-Metabase 버전은 짝으로 고정(1.5.3.0 = Metabase 59).
- **함정 2 — 연결 대상**: dbt의 DuckDB 파일 직결은 읽히긴 하지만 서빙 계층 실격.
  파일은 프로세스 간 단일 쓰기라 같은 호스트에선 transform이 잠금 충돌로 죽고
  ("Conflicting lock is held" 실측), 컨테이너 경계(virtiofs)에선 잠금이 전파되지 않아
  쓰기 도중 읽기가 무방비가 된다(실측 — 더 나쁨). → DAG 끝에 `publish` 태스크를 달아
  마트를 DuckLake로 발행하고, Metabase는 DuckLake만 read-only로 읽는다(동시성 중재를
  파일 잠금이 아니라 PG 트랜잭션이 한다).
- **함정 3 — 커넥션 풀과 init_sql**: init_sql의 `CREATE OR REPLACE SECRET`이 동시 카드
  로딩 때 write-write conflict를 낸다(실측). S3 자격증명은 세션 로컬 `SET s3_*`로.
- 대시보드 1장: 악화 쿼리 랭킹 표(first→last avg latency·악화율) + 일별 추이 +
  인스턴스 필터. 질문·필터 배선까지 API로 생성(멱등).

**실측**: 4태스크(offload→gate→transform→publish) 컨테이너 안 e2e success ·
마트=API=화면 3자 수치 일치(instance 8, +149.1%) · 발행(쓰기) 중 연속 41회 읽기 무중단.
**산출물**: VERIFICATION 8절 · RUNBOOK 5절 · 블로그 7편.

---

## Phase 8 — 감사 결함 소탕: 아카이브가 자신을 지우는 경로

**상황**: 코드 감사가 확정 결함 넷을 잡았다. 전부 "장치가 자기 원칙을 자기한테는
적용하지 않은" 뒷면들 — 그중 F1은 아카이브의 존재 이유를 부정하는 치명 결함.

**결함과 수정**:
- **F1 (치명) — 아카이브 자기파괴**: offload의 delete-first 멱등 재적재에서 삭제가
  "원천 0행" 체크보다 먼저 실행됐다. 원천 보존(7일) 밖 dt를 backfill/Clear로
  재실행하면 유일본 parquet 삭제 후 아무것도 안 쓰고 exit 0(가짜 파티션으로 실측
  재현). → 원천 0행 + 파티션 존재 시 삭제 없이 `ArchiveSelfDestructError`로
  시끄럽게 실패(재시도·webhook 경로 탑승). 원천 0행 + 파티션 없음은 스킵,
  정상(N행)은 기존 멱등 경로 유지. fail-closed를 쓰기 경로에도.
- **F2 — 게이트의 Seq Scan**: quality/verify_count가 captured_at 단독 필터로 원천
  전체 스캔(EXPLAIN 실측 332ms/31k버퍼) — offload가 지킨 인덱스 선두 원칙을 게이트가
  위반. → 레지스트리 인스턴스별 등치 루프로 통일(Index Only Scan 20ms/76버퍼).
- **F3 — publish 혼합 버전**: 마트 2개를 개별 커밋 → 중간 실패 시 "새 fct + 어제
  mart" 혼합 노출(장애 주입 실측). → DuckLake 단일 트랜잭션(BEGIN…COMMIT)으로 발행,
  실패 시 롤백(새 스냅샷 0개 실측).
- **F4 — 유지보수의 데모 의존**: measure()가 데모 산출물 query_snapshot 하드 참조 —
  새 환경에서 주간 DAG 즉사(실측). → 존재 테이블 목록 기반 계측(마트 포함), 테이블
  0개면 정리만. run_demo의 DROP TABLE에 확인 가드(--force/DUCKLAKE_DEMO_FORCE/y).
- **추가**: 게이트 4축(스키마 드리프트 — 유실·타입 변경 FAIL, 초과 컬럼 WARN),
  알림 payload에 대시보드 URL, `tests/` 신설(pytest 35개 — 게이트 판정·F1 가드·
  offload 경계·발행 원자성 고정).

**실측**: F1/F3 전·후 대비, F2 EXPLAIN 전·후, pytest 35 passed, 닫힌 창 재검
ALL MATCH(07-05=149,259 / 07-06=79,894) — VERIFICATION 9절.
**잔여**: CI 배선(테스트 강제), 부분 유실 가드(0행이 아닌 급감), parquet 방향
스키마 검사 — 블로그 8편.

---

## 감사 백로그 (Phase 8 감사에서 정리)

코드 감사가 남긴 항목의 처분을 한곳에 못박는다 — "다음에 한다"와 "안 한다"를
구분하고, 안 하는 것엔 이유를 단다.

### 다음에 할 것 (우선순위 순)

1. **CI 배선 + dbt unit tests** — pytest 35개는 로컬 자산까지다. 커밋마다 강제
   (GitHub Actions)하고, 델타 로직(순리셋·클램프)은 dbt unit test로도 고정.
2. **deadman 알림(heartbeat)** — 지금 알림은 "실패하면 운다"다. 스케줄러가 통째로
   죽으면 아무도 안 운다. 파이프라인 성공이 주기적으로 heartbeat를 찍고, 끊기면
   외부에서 경보하는 역방향 감시.
3. **365dt 규모 실측 → microbatch 증분** — 마트는 전체 재빌드다. 1년치(365dt)를
   합성 적재해 재빌드 시간·게이트 시간을 실측하고, **그 수치를 근거로** dbt
   microbatch 증분 전환을 판단한다(수치 없이 미리 최적화하지 않는다).
4. **mart 롤링 윈도우 재설계** — mart_query_regression이 "전체 구간 첫날 vs
   마지막날" 비교라 적재가 길어질수록 의미가 흐려진다. 최근 N일 롤링 창으로.
5. **dbt contracts** — 마트 스키마를 계약으로 선언해 다운스트림(Metabase 카드)이
   기대는 컬럼·타입 변경을 빌드 시점에 잡는다.
6. **운영 대시보드화** — 게이트 FAIL·마지막 성공 dt·발행 지연을 Metabase 상태
   카드로. 알림(webhook)과 화면(대시보드)의 이원화 해소.

(스키마 드리프트 게이트 4축은 백로그였으나 Phase 8에서 구현 완료 — 유실·타입 변경
FAIL, 초과 컬럼 WARN.)

### 안 하기로 한 것 (이유와 함께)

- **OpenLineage/Marquez**: 계보 소비자가 나 하나다 — dbt docs의 문서 계보로 충분.
  계보를 질의할 팀이 생기면.
- **Cosmos(dbt→Airflow 태스크 분해)**: 모델 3개짜리 프로젝트에 태스크 그래프 분해는
  오버헤드만 추가. 모델 수십 개·부분 재시도 요구가 생기면.
- **elementary(dbt 관측성)**: 자체 4축 게이트 + webhook과 역할이 겹친다. 도구를
  늘리기보다 게이트를 키운다.
- **dbt source freshness**: 게이트 freshness 검문과 중복 — 같은 판정을 두 군데서
  내리면 기준이 갈라진다(게이트가 단일 진실).
- **MinIO ILM(수명주기 정책)**: raw는 "지우지 않는 것"이 존재 이유다. 용량이 문제가
  되는 시점에 티어링(cold storage)으로 — 삭제 정책은 마지막 수단.
- **Kafka**: 원천이 일 배치 스냅샷이라 **이벤트 스트림이 아니다** — 붙일 스트림
  자체가 없다. 준실시간 신선도 요구가 생기면 원천 PG에 CDC(Debezium)→Kafka 아크로
  가는 게 맞고, 그때도 이 저장소가 아니라 별도 수집 계층이다.
- **Spark**: 단일 노드 DuckDB로 수년치 처리 가능(컬럼나+파티션 프루닝). 메모리+로컬
  디스크 한계를 실측으로 넘으면.
- **Iceberg/Delta**: 멀티엔진(Spark·Trino·Flink)이 한 테이블을 공유하는 조직 표준.
  단일 엔진(DuckDB)인 우리 규모엔 DuckLake가 맞고, 전환은 어댑터 문제.
- **클라우드 DW(BigQuery)**: 로컬 재현성 우선. dbt 어댑터 교체로 이전 가능.
- **컬럼 레벨 계보·PII 거버넌스**: dbt Enterprise 영역. 문서 계보까지만.

---

## 블로그 계획

새 시리즈(카테고리 분리, DBTower와 별개). 0편(왜: 버려지는 7일)→1~5편(Phase별). 각 편 개선 아크.
DBTower 0편에서 "관측 데이터의 다음 여정"으로 상호 링크.

## Sources (전제·함정 검증)
- [AWS PI 7일 무료 보존](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PerfInsights.Overview.cost.html)
- [Airflow data intervals 심화](https://towardsdatascience.com/airflow-data-intervals-a-deep-dive-15d0ccfb0661/)
- [Airflow backfill 함정](https://risingwave.com/blog/avoiding-airflow-backfill-pitfalls-expert-advice/)
- [Airflow 멱등 dbt 태스크](https://tomasfarias.dev/articles/writing-idempotent-dbt-tasks-for-airflow/)
- [DuckLake 공식](https://ducklake.select/) · [dbt Model governance](https://docs.getdbt.com/docs/mesh/govern/about-model-governance)
- [추출 best practice(CDC/replica)](https://www.automq.com/blog/fivetran-vs-airbyte-elt-tools-comprehensive-comparison)
