# dbtower-lakehouse 로드맵 — 상황 주도 개선 아크

> 이 문서는 기능 나열이 아니라 **"어떤 상황에서 무엇이 깨지고, 그래서 무엇을 만드는가"**로 쓴다.
> 각 단계 = (상황 가정) → (그 상황에서 드러나는 한계) → (판단) → (개선) → (라이브 실측·스크린샷) → (정직한 잔여).
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

## 공통 가드레일 (전 단계 — 위반 시 실패)

1. **분석이 운영의 부하가 되면 안 된다** — DBTower의 A9 원칙을 계승. 메타 PG 추출은 읽기 전용 +
   시간창(어제) + 배치 크기 제한. **운영 대상 DB에서 직접 뽑지 않는다**(부하). 관측 전용인 메타 PG에서만.
   업계 정석(CDC/read replica)과 같은 이유 — [Fivetran/Airbyte 추출 best practice](https://www.automq.com/blog/fivetran-vs-airbyte-elt-tools-comprehensive-comparison).
2. **멱등성** — 같은 논리 날짜를 몇 번 돌려도 결과 동일(파티션 덮어쓰기/upsert). backfill 안전.
   [Airflow 멱등성 원칙](https://tomasfarias.dev/articles/writing-idempotent-dbt-tasks-for-airflow/).
3. **정직 표기** — 지연 도착·중복·품질 실패·근사는 감추지 않고 메트릭/노트로.
4. **실측 필수** — 단계마다 로컬 e2e + 스크린샷(docs/images/) + docs/VERIFICATION.md 절 기록.
5. **도메인 섞기 금지** — 다른 프로젝트(pay 등)의 성과를 이 프로젝트 커버리지로 계산하지 않는다.
   pay의 Kafka는 결제 이벤트지 이 파이프라인이 아니다.
6. **범위 정직** — Kafka 스트리밍·Spark·클라우드 DW는 현 규모(일 수만 행)에 과잉. 잔여에 "언제 필요한가"와 명시.
7. **관례** — 커밋 한국어, 이모지 금지, 블로그 개선 아크.

---

## 0단계 — 계약 먼저 (스캐폴드)

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
**잔여**: 아직 데이터 안 흐름 — 1단계에서.
**산출물**: VERIFICATION 1절 · 블로그 0편 "버려지는 데이터에서 시작하는 파이프라인 — 계약 먼저".

---

## 1단계 — Extract & Load (EL)

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
- **멱등성**: 파티션 통째 덮어쓰기. 같은 날짜 2회 실행해도 행수 불변.
  (실행 기록: 구현은 "임시 경로 후 교체"가 아니라 **delete-first→write** — 그 delete가 만든
  자기파괴 경로를 8단계 가드(`decide_partition_action`)로 막았다. 결과적 멱등성은 동일.)
- **함정(웹 검증)**: backfill은 수백 태스크를 동시 실행해 스케줄러·워커·downstream을 짓누를 수 있다 —
  [backfill 자원 함정](https://risingwave.com/blog/avoiding-airflow-backfill-pitfalls-expert-advice/). 동시성 상한.
  parquet 스키마를 명시 선언(조용한 타입 추론 변화 차단).

**실측(라이브)**:
- e2e: DBTower 하루 돌린 실제 스냅샷 → DAG 트리거 → MinIO에 parquet →
  DuckDB `SELECT count(*) FROM read_parquet('s3://...')`가 원천 행수와 일치.
- 멱등: 같은 날짜 2회 → 행수 불변(중복 0).
- **도그푸딩**: 추출 중 메타 PG 부하를 DBTower 자신으로 관측(파이프라인이 준 부하를 관제탑이 감시하는 순환).
**잔여**: raw는 질문에 못 답함 — 2단계.
**산출물**: VERIFICATION 2절 · 스크린샷(Airflow 성공 런·MinIO·DuckDB 카운트) · 블로그 1편.

---

## 2단계 — Transform (dbt)

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
**잔여**: 조용히 틀린 데이터 위험 — 3단계.
**산출물**: VERIFICATION 3절 · 블로그 2편 "누적 스냅샷을 일간 델타로".

> 실행 기록(2026-07-15 코드 대조): 실존 모델은 `stg_query_snapshot`·`fct_query_daily`·
> `mart_query_regression` 3개. `mart_regression_trend`는 **`mart_query_regression`으로
> 개명·구현**됐고 10단계에서 롤링 창(7 vs 30)으로 재설계됐다. `mart_engine_compare`
> (기종별 비교)는 **미구현** — 며칠 시야의 기종 비교는 답이 얕아, "기종별 **장기** 성능·용량
> 비교(fleet)"로 성격이 바뀌어 "dbtower 패밀리" 절의 lakehouse 후보로 이월했다.

---

## 2.5단계 — 거버넌스·카탈로그 (dbt 네이티브)

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

> 실행 기록(2026-07-15 코드 대조): **절반만 이행됐다.** 충족 — 모델·컬럼 description(전 모델),
> 데이터 계약(`contract: enforced` — 단 이 단계가 아니라 **9단계에서** 구현). 미이행 —
> `access`/`group`은 강제할 소비자 팀이 없는 1인 저장소에서 실익 0이라 **안 한다**(팀 소비자가
> 생기면), `exposures`(대시보드 의존 선언)는 저비용 잔여 후보로 남긴다. dbt docs 사이트는
> 로컬 1회 생성뿐 재현 배선 없음. 이 단계를 "완료"로 읽지 말 것.

---

## 3단계 — 데이터 품질 게이트

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

> 실행 기록(2026-07-15 코드 대조): 실제 구현은 본문의 "dbt tests + source freshness"가 아니라
> **자체 Python 4축 게이트**(`extract/quality.py` — 정합·완결성·신선도·스키마 드리프트,
> OK/WARN/FAIL + FAIL만 차단)다. dbt tests는 transform 단계에서 병행하고, source freshness는
> 게이트 신선도 검문과 중복이라 배제(백로그 "안 하기로 한 것" — 같은 판정을 두 군데서 내리면
> 기준이 갈라진다). 웹훅은 게이트 FAIL → 태스크 실패 → `on_failure_callback` 경유.

---

## 4단계 — Serve (질문에 답하기)

**상황**: 이 모든 게 결국 "지난 30일 가장 나빠진 쿼리는?"에 답하려는 거였다. 답을 눈에 보이게.

**개선(구현)**: DuckDB 애드혹 + 경량 대시보드 1장(Evidence 또는 Metabase 하나만 — 판단 기록):
"인스턴스별 30일 악화 쿼리 TOP 10 + 추세선".

**실측(핵심 대비)**: **DBTower(7일 시야)로는 "데이터 없음", lakehouse(30일 시야)로는 답이 나오는 것을
나란히 스크린샷** — 이 대비가 프로젝트 전체의 존재 증명.
**산출물**: VERIFICATION 5절 · 블로그 4편.

> 실행 기록: 이 Serve 단계는 품질 게이트(블로그 4편)·DuckLake(5편)·운영 경화(6편)를
> 먼저 닫은 뒤 **7단계(아래)에서 Metabase 대시보드로 구현**했다 — 반쪽 데이터 위에
> 화면부터 얹지 않으려는 순서 조정.

---

## 5단계 — 테이블 포맷(DuckLake): "lake"를 "lakehouse"로

**상황**: mart 적재가 도중일 때 대시보드를 열었더니 반쪽 데이터가 보였다. "지난주 기준으로 다시 계산해줘"는
아예 불가능했다. 컬럼 하나 추가했더니 옛 파티션 읽기가 깨졌다.

**한계 인지**: 1~4단계의 "parquet 파티션 덮어쓰기"는 lake다. lakehouse의 정의(개방 포맷 위에
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

## 6단계 — 운영 경화: 실패해도 아무도 모르는 파이프라인은 미완성

**상황**: 3단계의 fail-closed 게이트가 반쪽 데이터를 잘 막았다. 그런데 막았다는 사실을
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

## 7단계 — 대시보드: 마트는 있는데 소비자가 없다

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

## 8단계 — 감사 결함 소탕: 아카이브가 자신을 지우는 경로

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

## 9단계 — 신뢰할 수 있는 파이프라인: 커밋·침묵·계약

**상황**: 8단계에서 tests/를 열어 pytest 35개로 로직을 고정했다. 그런데 그건
**로컬 자산**이다 — 내 노트북에서만 돈다. 커밋이 그걸 강제하지 않으면 며칠 뒤
누군가(나 포함)가 테스트를 깨고도 초록불이라 착각한다. 그리고 알림은 여전히
"실패하면 운다"뿐인데, 감사가 실제로 지적한 사건은 정반대였다 — 원천 수집기가
**21시간 침묵**했는데 아무 알림이 없었다(태스크가 시작조차 안 했으니 실패
콜백도 없었다). 마지막으로 마트 스키마는 계약이 없어서, 컬럼 타입을 바꿔도
발행 전까지 아무도 모른다(대시보드 카드가 런타임에 깨질 뿐).

**한계 인지**: 세 구멍은 서로 다른 방향이다. (1) 테스트가 로컬에만 있으면 회귀를
못 막는다. (2) "실패 감지"로는 '미실행'을 절대 못 잡는다 — 침묵은 성공의 부재로만
잡힌다. (3) 스키마는 코드가 아니라 데이터의 형태라, SQL 리뷰로는 안 걸린다.

**개선(구현)**:
- **CI(GitHub Actions)** — `.github/workflows/ci.yml` 3관문: ruff(린트) + pytest +
  dbt(deps/parse/build). 이 스택의 강점을 CI에서 쓴다 — 쿼리 엔진 DuckDB가 임베디드라
  MinIO·PG 없는 러너에서 tiny 픽스처 parquet(`scripts/ci_fixture.py`)로 dbt build를
  e2e로 돌린다(staging→fct→mart + 데이터 테스트 + 계약 + unit test 전부 러너 안에서).
  소스 위치는 `RAW_SNAPSHOT_LOCATION` 환경변수로 스왑(운영 기본=MinIO 불변). 배지 README.
- **dbt unit tests** — 델타 로직 엣지 4건을 정적 입력→기대 출력으로 고정: first-vs-last
  차분, 순리셋 GREATEST(0,..) 클램프, 하루 1스냅샷 델타 0, 지문 충돌 SUM. 입력을 목킹하므로
  실데이터 없이 CI에서 완결. dbt-duckdb 제약(외부 read_parquet 소스 introspect 불가)은
  픽스처 뷰 등록으로 우회 — 미지원이 아니라 외부 소스 introspect의 문제(정직 표기).
- **deadman 알림(heartbeat)** — 성공 시 heartbeat를 카탈로그 PG(ducklake_catalog,
  서비스 추가 0, 메타 DB 비오염)에 남기고(DAG 마지막 태스크), `extract/deadman.py`가
  "기한 내 갱신 없으면 경보"하는 역방향 감시. 두 경로: Airflow `deadman_watch`
  DAG(@hourly, 스케줄러 사는 동안) + 외부 cron(`python -m extract.deadman`, 스케줄러
  total death까지). 경보는 기존 webhook 재사용. Airflow /health 폴링보다 이 패턴이
  '미실행'까지 잡는다.
- **dbt contracts** — fct·mart에 `contract: enforced: true` + 컬럼 name/data_type/
  constraint 선언. dbt-duckdb가 DB 레벨로 실제 enforce(클램프 delta>=0을 CHECK로).
  발행 전 마지막 방어선.

**실측(라이브)**: CI 3관문 로컬 재현(ruff pass·pytest 53·dbt build PASS=25) · unit test 4
PASS(엣지 4종) · 계약 위반 주입 시 빌드 ERROR("data type mismatch") → 원복 PASS ·
deadman 30h 침묵 → 경보 발화 수신(로컬 :18809 HTTP 200)·미실행 DAG 경보 · 회귀 없음
(verify ALL MATCH 149,259/79,894, 실데이터 계약 강제 dbt run PASS=3). VERIFICATION 10절.

**잔여**: 규모 실측(365dt)·롤링 윈도우·운영 대시보드화는 다음(아래 백로그) — VERIFICATION 11절.

---

## 10단계 — 규모와 서빙: 며칠치로는 "버틴다"를 증명 못 한다

**상황**: 지금까지 모든 실측은 닫힌 dt 3개(수십만 행)에서 돌았다. 그 규모에선
전부 초 단위라 "규모에서도 버틴다"고 말하고 싶어진다. 하지만 그건 증명이 아니라
희망이다 — 마트(fct)는 매일 O(전체 이력)을 다시 계산하는데, 이력이 3일이라 안 아팠을
뿐이다. 1년치면 어떤가? 그리고 mart_query_regression은 "전체 이력 첫날 vs 마지막날"
비교라, 이력이 길어질수록 "1년 전 대비"가 되어 0편의 질문("지난달 대비")과 어긋난다.
마지막으로 파이프라인 상태는 알림(실패)·heartbeat(성공의 부재)로만 보는데, "지금
전반적으로 건강한가"를 한 화면으로 볼 곳이 없다.

**판단 — 먼저 재보고, 수치가 요구할 때만 최적화**: 감이 아니라 1년치를 실제로 만들어
어디가 먼저 무너지는지 잰다. 그 수치가 증분 전환을 정당화하면 하고, 아니면 "지금은
안 한다"를 수치로 정당화한다(문제 없는 곳 최적화 금지).

**개선(구현)**:
- **365dt 합성 규모 실측** — 닫힌 dt parquet를 날짜 시프트 복제해 365dt×6인스턴스=
  2,190파일(54.5M행)을 격리 프리픽스(`scale/`)에 생성(`scripts/scale_synthesize.py`,
  실데이터·원천 무접촉, 끝나고 정리). 병목을 수치로 지목: **fct 전체 재빌드 407.62s가
  유일한 병목**, 나머지(mart 0.31s·게이트 per-dt 8–22ms·CHECKPOINT 0.47s·글롭 2.2s)는
  초 단위. 파일 평균 177KB로 128MB 타깃의 1/741(소파일 폭증 계측).
- **증분 전환(delete+insert)** — 407s가 정당화한다. fct grain이 dt 독립이라 새 dt만
  계산해 append/replace. `unique_key=(instance,query,dt)`, 컴파일 타임 워터마크
  리터럴로 hive 파티션 프루닝(스칼라 서브쿼리론 프루닝 실패 — 실측). **407.62s →
  4s(~100배)**. microbatch는 event_time·unique_key 제약이라 delete+insert 선택(정직).
- **롤링 윈도우 재설계** — 최신 dt 기준 최근 N일 vs 직전 M일(기본 7 vs 30) 미끄러지는
  창. 365dt(최근 7일 악화 주입)에서 rN=7·pN=30 정확, 주입 계층이 랭킹 분리(실측).
  실데이터 3dt에선 0행(이력 부족 — 정직하게 빈다).
- **운영 대시보드화** — publish/heartbeat가 매 런 메타를 `pipeline_run_log`(DuckLake)로
  발행 → Metabase 운영 대시보드(마지막 성공 dt·오늘 게이트 상태·최근 런). 분석
  대시보드와 이원화. 07-08(데이터 없는 날) FAIL을 실제로 잡는 화면 실측.

**실측**: VERIFICATION 11절(규모 수치표·증분 전/후·롤링 랭킹·운영 카드). pytest 57 ·
dbt build PASS=26(unit test 5) · verify ALL MATCH · 합성 데이터 정리 완료.

**잔여**: 과거 dt 정정은 --full-refresh 필요. 롤링은 이력이 쌓여야 실데이터에서 참.
고유 쿼리 폭증(카디널리티)은 복제라 미재현 — VERIFICATION 12절.

---

## 11단계 — 남이 띄우게: 데모 위성에서 셀프호스트 애드온으로

**상황 가정**: DBTower를 셀프호스트하는 다른 사람이 이 저장소를 보고 "나도 7일 뒤 사라지는
쿼리 이력을 장기 보관하고 싶다"며 `git clone` 후 `docker compose up`을 쳤다. **아무것도
안 뜬다** — compose가 `networks.default: dbtower_default (external: true)`로 *내* 데모 스택이
이미 떠 있음을 전제하고, 원천 호스트명(`dbtower-postgres`·`dbtower-minio`)·크리덴셜
(`airflow/airflow`·`dbtower1234`)이 전부 하드코딩돼 있기 때문이다.

**한계 인지**: 지금까지(0~10단계)의 모든 실측은 "내 노트북에서 내 데모 스택 옆"이라는
단일 환경 전제 위에 서 있었다. 그건 파이프라인이 *동작함*을 증명했지, *남이 띄울 수 있음*을
증명하지 않는다. 셋이 서로 다른 방향의 구멍이다: (1) **결합** — 내 데모 스택에 물리적으로
얹혀 있어 독립 기동 불가. (2) **시크릿** — 데모 평문 크리덴셜이 코드에 박혀 있어, 외부 노출
순간 전부 취약점이 된다(Airflow 웹서버는 기본 인증이 없고, 과거 CVE는 로그·UI로 시크릿이
샌 이력이 있다 — [Airflow security](https://airflow.apache.org/docs/apache-airflow/stable/security/index.html),
[미설정 Airflow 노출 사고](https://www.darkreading.com/vulnerabilities-threats/misconfigured-apache-airflow-platforms-threaten-organizations)).
(3) **재현·문서** — "만든 사람만 아는" 포트·연결·부트스트랩 절차는 6단계 RUNBOOK이
장애 대응엔 있어도, *처음 띄우는 남*을 위한 설치 계약은 없다.

**제품 형태 확정 — 어플라이언스(배터리 포함 상자), 조립식 아님**: 셀프호스트한다는 것은
그 툴의 스택을 통째로 받는다는 뜻이다(Grafana 셀프호스트 = 저장 엔진 동봉, GitLab = PG·Redis
동봉 — 아무도 "Redis 굴리기 싫다"고 하지 않는다. 상자 안에 있어 안 보이기 때문). 사용자가
싫어하는 것은 "DuckDB를 굴리는 것"이 아니라 "DuckDB를 이해·운영하는 것"이므로, 답은 DuckDB를
빼는 게 아니라 **안 보이게** 만드는 것이다. 사용자가 하는 일은 `.env`에 자기 DBTower를 적고
`docker compose up` → Metabase 대시보드 열기뿐. DuckDB/DuckLake/Airflow는 영원히 안 만진다.
결과를 자기 창고(Snowflake/BigQuery/자기 PG)에 넣고 싶은 소수는 dbt 어댑터 교체로 열려 있으나
(아래 "안 하기로 한 것"의 DW 항목·12단계 개방 포맷 escape hatch), **지금 구현하지 않는다** —
어플라이언스 사용자의 다수는 번들로 만족하고, 부품 선택을 강요하는 것이 오히려 셀프호스트 UX를
해친다(Sentry·Plausible·Grafana가 인기인 이유 = 생각 안 하게 함).

**부트스트랩 형태 확정 — 한 파일 + compose profile(사례 기반)**: 성공한 셀프호스트 툴은
"빈 화면으로 시작하지 않는다" — Grafana 데모 compose는 Prometheus를 형제로 동봉하고, Metabase는
Sample DB를 내장한 뒤 위저드로 진짜 연결을 유도한다(데모↔프로덕션 경로 분리). 따라서
`docker-compose.standalone.yml` 한 벌에 Docker Compose `profiles`로 두 경로를 가른다:
기본 `up`은 `.env`의 진짜 DBTower 메타 PG에 붙고(프로덕션), `--profile demo up`은 샘플 원천
PG(기존 `scripts/ci_fixture.py` 픽스처 시드 재사용 — 추가 비용 낮음)를 동봉해 DBTower 없이도
전체 e2e를 즉시 시연한다(평가·데모). 양자택일이 아니라 한 파일에서 프로필로 분기.

**판단 — 범용 도구로 넓히지 않는다. DBTower 애드온으로 셀프호스트화만 한다**:
"아무 Postgres/DB에나 붙는 범용 쿼리 분석 도구"는 이미 성숙한 레드오션이다 —
Percona PMM(QAN)·pgwatch·OpenObserve(Parquet+S3+DataFusion, 이 스택과 구조가 거의 동일)·
pganalyze. solo 프로젝트가 여기 뛰어들면 "OpenObserve의 열등한 재구현"이 된다
([pganalyze 대안](https://uptrace.dev/tools/postgresql-monitoring-tools),
[OpenObserve](https://openobserve.ai/)). **DBTower와의 결합은 약점이 아니라 이 프로젝트만의
자산이다** — 관측 플랫폼(DBTower)과 그 장기 기억 레이어를 둘 다 소유한 구조는 드물고,
그게 Prometheus↔Thanos 관계 그대로다(DBTower는 이미 Prometheus+Grafana를 품는다 —
README). 따라서 "실제 사용자"의 정직한 정의는 *아무나*가 아니라 **DBTower를 셀프호스트하는
사람**이고, 목표는 "남이 자기 DBTower 프로덕션에 이걸 붙일 수 있게"까지다.

**개선(구현 계획)** — 다섯 축. 지금 스택의 강점(임베디드 DuckDB·서비스 최소)을 유지한 채
결합만 걷어낸다:

1. **결합 분리 (핵심 장벽)** — compose의 external 네트워크 전제와 원천 호스트명 하드코딩을
   제거. 원천 접속(PG·S3)을 전부 `.env` 변수로 주입받게 하고, 두 가지 배치 형태를 제공:
   - `docker-compose.yml`(기존, 개발용) — 내 데모 스택에 붙는 현행 유지.
   - `docker-compose.standalone.yml`(신규) — 자체 MinIO·카탈로그 PG를 번들해, 남의 DBTower
     *데이터 소스*(메타 PG)만 `.env`로 가리키면 뜨는 독립 스택. 개발용 bind-mount 볼륨
     (`./dags`·`./extract`)은 프로덕션에서 이미지에 구워(6단계 Dockerfile 재사용) 코드
     마운트를 제거한다(호스트 코드 마운트는 재현성·보안 위험 —
     [production compose best practice](https://nickjanetakis.com/blog/best-practices-around-production-ready-web-apps-with-docker-compose)).
2. **시크릿 — 데모 평문 제거** — `airflow/airflow`·`dbtower1234`·MinIO 키를 기동 시
   생성(또는 필수 입력)으로. 특히 Airflow는 `fernet_key`(커넥션 암호화)와 `webserver secret_key`를
   **반드시 새로 생성**해야 한다 — 기본값·공유값 사용은 공식 권장 위반이다
   ([Fernet](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html)).
   `.env`를 시크릿 단일 진실로 두고, `.env.example`엔 값 대신 placeholder + 생성 명령을 적는다.
   `.gitignore`에 `.env` 확인(현재 커밋 안 됨 — 유지).
3. **독립 실행 위생** — `restart: unless-stopped`는 이미 있음(유지). 이미지 태그를 `:latest`에서
   버전 고정으로(두 호스트가 다른 코드를 도는 사고 방지 —
   [image tagging 함정](https://distr.sh/blog/running-docker-in-production/)). healthcheck를
   *동작에 연결*(현재 airflow-postgres만 있음 → webserver/scheduler에도, 실패 시 재기동).
   Metabase 앱 DB는 지금 H2(볼륨 영속)인데 **프로덕션은 Postgres로 이관**해야 한다(공식이
   H2 프로덕션 사용을 말림, 이관 시 버전 일치 함정 —
   [Metabase H2→PG](https://www.metabase.com/docs/latest/installation-and-operation/migrating-from-h2)).
   카탈로그 PG를 이미 쓰므로 Metabase 앱 DB도 거기 별도 DB로 흡수하면 서비스 추가 0.
4. **노출·인증·TLS (외부에 열 때)** — Airflow(8080)·Metabase(13001)는 리버스 프록시
   (Caddy/nginx) 뒤에서 TLS + 인증으로만 노출하고, **MinIO(9000)·카탈로그 PG는 절대 공개
   금지**(내부 네트워크 전용). Caddy는 ACME로 인증서 자동 발급·갱신이라 단일 노드
   셀프호스트에 맞다. TLS 1.2/1.3만, HSTS, 보안 헤더
   ([리버스 프록시로 감싸기](https://medium.com/@impiyush/stop-exposing-your-self-hosted-services-do-this-instead-6e327a0c69a0)).
   이건 "인터넷에 열 사람만" 하는 선택 계층 — 사내 네트워크면 프록시까지만.
5. **릴리스·설치 계약(문서)** — **LICENSE 파일 추가가 0순위**(현재 부재 — 라이선스 없는 공개
   저장소는 기본값이 All Rights Reserved라 남이 법적으로 셀프호스트할 수 없다. DBTower P0-1과
   동일 갭, 같은 라이선스로 정렬). 이어 README에 "**DBTower에 연결하기**" Quick start:
   `query_snapshot` 테이블을 어떤 **읽기 전용** 권한으로 읽는지(공통 가드레일 1: 원천 무부하),
   `.env`에 채울 최소 변수, `docker compose -f docker-compose.standalone.yml up`. 카탈로그
   PG 백업/복원 절차(RUNBOOK 확장 — 카탈로그가 날아가면 DuckLake 메타가 소실). 버전 태그
   릴리스. 두 저장소 상호 링크(DBTower README → "장기 보관은 dbtower-lakehouse").

### 착수 명세 (Opus) — 11단계

> **구현 담당: Opus. 파일 단위 스펙.** 원칙: 기존 데모 compose(`docker-compose.yml`)는 불변
> (개발 경로 유지), standalone은 별도 파일. 모든 시크릿은 `${VAR:?}` 필수화 또는 생성 안내.
> 커밋 한국어·이모지 금지·실측 후 VERIFICATION 절 추가.

| # | 조각 | 재활용 자산 | 구현 명세 | 검증 기준 |
|---|---|---|---|---|
| S1 | `LICENSE` + `NOTICE` | — | Apache-2.0 전문(연도·저작자). DBTower와 같은 라이선스로 정렬. NOTICE에 번들 컴포넌트(Airflow·Metabase·DuckDB MotherDuck 드라이버) 고지 | 파일 존재, README 뱃지/절 |
| S2 | `docker-compose.standalone.yml` | 기존 `x-airflow-common` 패턴, `Dockerfile`, `metabase/Dockerfile` | 서비스: `minio`(+named volume)·`minio-init`(mc로 `lakehouse` 버킷 생성 후 종료)·`catalog-postgres`(DB `ducklake_catalog`)·`airflow-postgres`·`airflow-init/scheduler/webserver`·`metabase`. **external 네트워크 없음**(자체 default). 원천은 서비스로 안 띄우고 `.env`의 `SRC_PG_*`만 참조. 코드 bind-mount 제거 — `Dockerfile`에 `COPY dags/ extract/ dbt/` 추가(재현성). 이미지 태그 버전 고정(`:latest` 금지). 전 서비스 healthcheck+`restart: unless-stopped` | (a)(b) + `docker compose -f docker-compose.standalone.yml config`에 `external:` 0건 |
| S3 | 시크릿 배선 | S2 | `AIRFLOW__CORE__FERNET_KEY=${AIRFLOW_FERNET_KEY:?}` · `AIRFLOW__WEBSERVER__SECRET_KEY=${AIRFLOW_WEBSERVER_SECRET_KEY:?}` · admin 계정 `${AIRFLOW_ADMIN_USER:?}/${AIRFLOW_ADMIN_PASSWORD:?}`(airflow-init) · MinIO `${MINIO_ROOT_USER:?}/${MINIO_ROOT_PASSWORD:?}` · 카탈로그 PG `${DUCKLAKE_CATALOG_PASSWORD:?}`. `.env.standalone.example`에 placeholder + 생성 명령 주석(fernet: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`, 그 외 `openssl rand -hex 32`) | (c) — 저장소 grep에 데모 평문(`dbtower1234`·`airflow/airflow`) 신규 0건 |
| S4 | Metabase 앱 DB H2→PG | `metabase/Dockerfile`, S2의 catalog-postgres | `MB_DB_TYPE=postgres` + `MB_DB_DBNAME=metabase_app`(catalog-postgres에 별도 DB — init SQL로 생성) + `MB_DB_HOST/PORT/USER/PASS` env. 기존 데모 compose는 H2 유지(불변) | standalone 기동 후 Metabase 설정이 PG에 영속(재기동 후 유지) |
| S5 | demo 프로필 | `scripts/ci_fixture.py`의 `_ROWS`·`_SCHEMA` | `profiles: [demo]`로 `sample-source-postgres` 서비스 + `scripts/demo_seed.py`(신규): `_ROWS`를 재사용해 PG에 `database_instance`(id 2행)·`query_snapshot`(스키마는 CONTRACT §1) INSERT. ci_fixture는 parquet 생성이므로 **PG INSERT 변형 필요**(스키마 동일). demo 기동 시 `SRC_PG_HOST=sample-source-postgres` 오버라이드 | `--profile demo up` → offload 1런 성공 → Metabase 접속(전체 e2e, DBTower 없이) |
| S6 | README "DBTower에 연결하기" | CONTRACT §1(두 테이블), RUNBOOK | Quick start: ① 최소 권한 계정(예: `CREATE ROLE lakehouse_reader LOGIN PASSWORD '...'; GRANT CONNECT/USAGE; GRANT SELECT ON query_snapshot, database_instance TO lakehouse_reader;`) ② `.env.standalone.example` 복사·채움 ③ `up` ④ 검증 쿼리. **`database_instance` 권한 누락 시 "조용히 빈 결과" 함정 경고 필수**(CONTRACT §1 인용) | 문서만으로 제3자 재현 가능(검증 기준 (b)) |
| S7 | 노브 노출(12단계 연결) | 기존 env | `.env.standalone.example`에 `AIRFLOW__CORE__PARALLELISM`(추출 병렬)·`DUCKLAKE_RETENTION`·CHECKPOINT 주기 노브를 주석과 함께 — "몇백 대면 이 둘을 올려라" 가이드 | 문서화 확인 |

**함정(선검증)**: S2에서 `S3_ENDPOINT_HOSTPORT`는 컨테이너 관점 호스트명으로(기존 데모의
`dbtower-minio:9000` → standalone은 `minio:9000`). dbt `profiles.yml`은 이미 env 주입이라 불변.
`extract/config.py`는 이미 분리 완료(`DUCKLAKE_CATALOG_*` 폴백) — S2가 그 변수를 채우면 끝.

**검증 기준(실측 TODO — 아직 미구현, 충족 시 VERIFICATION 절 신설)**:
- (a) *깨끗한 환경 재현*: 이 저장소만 clone한 별도 머신(또는 격리 네트워크)에서
  `standalone` 스택이 내 데모 스택 없이 뜬다 — external network 참조 0.
- (b) *결합 분리*: 원천 호스트/크리덴셜을 `.env`로만 바꿔 임의의 DBTower 메타 PG를 원천으로
  붙여 offload 1런 성공(하드코딩 grep 0).
- (c) *시크릿 위생*: 저장소 전체에서 데모 평문 크리덴셜 grep 0(placeholder만). Airflow
  fernet/secret_key가 기동마다 환경별로 주입됨.
- (d) *노출 방어*: 프록시 뒤 Airflow/Metabase만 TLS로 접근 가능, MinIO/PG 포트는 외부
  바인딩 0(`docker compose port` 실측).

**잔여(정직 표기)**:
- **범용 도구화(아무 DB 소스)는 안 한다** — 위 판단. 원천은 "DBTower 메타 PG의 `query_snapshot`"
  계약에 고정. 다른 원천이 필요하면 그건 이 저장소가 아니라 별도 추출 어댑터.
- **Kubernetes/Helm 차트는 범위 밖** — 단일 노드 일 배치 규모에 오케스트레이터용
  오케스트레이터는 과잉(공통 가드레일 6). 멀티 노드·HA 요구가 실증되면.
- **멀티테넌시·SSO/RBAC 통합은 범위 밖** — 리버스 프록시 인증까지. 조직 IdP 연동
  (OAuth/LDAP)은 Airflow/Metabase 각자의 엔터프라이즈 설정 영역.
- **자동 시크릿 로테이션·Vault 연동은 범위 밖** — `.env` + 기동 생성까지. 시크릿 매니저가
  필요한 규모가 되면.

**산출물**: VERIFICATION 절(위 a~d) · RUNBOOK "설치·연결·백업" 확장 · 블로그
"데모 위성에서 남이 띄우는 애드온으로 — 결합을 걷어낸다".

> 실행 기록(2026-07-15 라이브 실측 — VERIFICATION 12절): **S1~S7 구현·검증 완료.**
> `LICENSE`(Apache-2.0)+`NOTICE`, `docker-compose.standalone.yml`(자체 MinIO·카탈로그 PG·
> Metabase 번들, external 네트워크 0), 시크릿 전부 `${VAR:?}`, Metabase 앱 DB H2→PG(번들 PG에
> 151테이블 영속), `--profile demo`(샘플 원천 PG 시드), 코드 이미지 굽기(Dockerfile COPY +
> `.dockerignore`), `.env.standalone.example`(생성 명령·노브·연결 가이드), README 셀프호스트 절.
> 검증 (a) `docker compose config` external:true=0 · (b) `--profile demo up` 7컨테이너 healthy →
> **DBTower 없이 offload→게이트(4축 OK)→dbt run PASS=3→데이터테스트 PASS=18** e2e, 번들 MinIO
> 10행/2dt/2instance 적재 · (c) 평문 시크릿은 데모 throwaway뿐 · (d) 격리 실행(dev 스택 무영향,
> 정리 후 잔존 0). **config.py 결합 분리(`DUCKLAKE_CATALOG_*`)가 그대로 채워져 동작.**
> 잔여: TLS/리버스 프록시(노출 시 선택 계층)·카탈로그 PG 백업 절차는 다음.

---

## 12단계 — 규모의 두 번째 축: 인스턴스 수(N) — 몇백 대를 관제하는 남

**상황 가정**: 11단계로 남이 셀프호스트를 시작했다. 그런데 그 사람의 DBTower는 6대가 아니라
**수백 대**를 관제한다. "내 규모에서도 이게 버티나?"

**한계 인지 — 이건 한 번도 안 재본 축이다**: 10단계의 규모 실측은 **시간축(dt)**만 늘렸다 —
6인스턴스 × 365일. **인스턴스축(N)은 6에 고정**돼 있었다. "몇백 대"는 직교하는 새 축이고,
여기엔 흔한 착각이 하나 있다: **N 증가 ≠ 파이프라인 서버 증가**. 이 파이프라인은 관제 대상
수백 DB에 직접 붙지 않는다(공통 가드레일 1). DBTower가 그 수백 대를 긁어 **메타 PG 한 곳**의
`query_snapshot`에 모으고, 이 파이프라인은 그 **한 테이블**만 읽는다. 따라서 N이 늘 때 커지는
것은 노드 수가 아니라 **행수·파티션 수**다.

**규모 공식**: 데이터 크기 = 인스턴스(N) × 고유쿼리(U) × 보존일(dt). 10단계 실측 베이스라인
(6대 = 365일 2,190파일·54.5M행 → 인스턴스-년당 365파일·9.08M행)을 N축으로 외삽:

| N (인스턴스) | 연 raw 파일 수 | 연 raw 행수 | 압축 크기/년 | 상태 판정 |
|---|---|---|---|---|
| 6 (실측) | 2,190 | 54.5M | ~0.4GB | 전부 초 단위(10단계) |
| 100 | 36,500 | ~908M | ~6GB | 소파일 압박 시작 |
| 300 | 109,500 | ~2.7B | ~19GB | 소파일 심각 · full-refresh 주의 |
| 500 | 182,500 | ~4.5B | ~32GB | 컴팩션·추출 병렬화 필수 |
| 1,000 | 365,000 | ~9B | ~63GB | 단일노드 유지, 노브 튜닝 |
| 3,000 (몇천 대 대기업) | 1,095,000 | ~27B | ~190GB | **단일노드 유지** · 컴팩션·추출 병렬화 필수 |

(몇천 대 대기업 셀프호스터가 전제여도 데이터는 low-TB — 단일노드 DuckDB 영역. 재설계가 아니라
컴팩션 스케줄·추출 병렬도 **설정값 두 개**를 노출하면 된다. 아래 개선 1·2가 그 노브다.)

**판단 — 감이 아니라 순서를 매긴다. 분산은 마지막 수단**: N을 실제로 늘려(10단계
`scale_synthesize`를 인스턴스축으로 재사용) 어디가 **먼저** 무너지는지 순서를 확정하고,
그 수치가 요구하는 것만 한다. 기존 "안 하기로 한 것"(Spark·Iceberg)의 유보 조건("규모가
요구하면")을 이 축이 구체화한다.

**개선(분석 — 순서대로 무너지는 breakpoint)**:

1. **소파일 폭증 (가장 먼저 무너진다)** — raw는 `dt=/instance_id=/`로 파티션돼 인스턴스당
   dt당 파일 1개. N=300이면 연 ~11만 파일, 여전히 평균 177KB(128MB 타깃의 1/741 —
   10단계 실측). 검색 근거: tiny 파일 10만 개면 쿼리 플래닝·`LIST`·manifest 추적이 전부
   느려지고 스토리지 비용이 튄다 — 소파일은 프로덕션 레이크하우스의 최다 구조 결함
   ([Iceberg 소파일 가이드](https://lakeops.dev/blog/iceberg-small-files-guide),
   [DuckDB+Iceberg 소파일 트랩](https://medium.com/@hadiyolworld007/duckdb-iceberg-without-pain-partitioning-compaction-and-the-small-files-trap-in-local-first-3686a6a86e12)).
   → **대응**: (a) 6단계 CHECKPOINT 컴팩션을 임계 기반으로 — 파일 수·평균 크기 대비 타깃이
   임계를 넘을 때만, 인스턴스 수에 비례해 공격적으로(검색: "streaming은 시간당 여러 번,
   weekly batch는 주 1회" — 워크로드별 임계). (b) 파티션 전략 재검토 — `instance_id`
   물리 파티션은 N=수백이면 파티션 자체가 폭증한다. `dt`만 파티션하고 instance는 파일 내
   정렬 컬럼(zonemap 프루닝)으로 낮추는 트레이드오프를 N=300에서 실측 비교.
2. **추출 처리량 (다음)** — offload는 인스턴스별 루프로 메타 PG를 N회 조회(1단계에서
   인덱스 선두 `instance_id`를 타려고 의도한 구조). N=수백이면 직렬 루프가 일 배치 창을
   넘길 수 있다. → **대응**: 병렬화하되 **메타 PG 부하 상한이 천장**(가드레일 1 — 관제탑을
   느리게 하면 자기모순). Airflow `PARALLELISM`(현재 4) 상향 + 인스턴스 배치, 단 원천
   동시 커넥션 상한을 넘지 않게. DBTower 자신이 워커 풀로 수집을 병렬화하는 것과 대칭 구조.
3. **단일 노드 DuckDB 천장 (그 다음)** — 증분 daily(9·10단계)는 **새 dt만** 계산해 N에
   선형이지만 절대량이 작다(하루치). 진짜 위험은 **full-refresh/backfill** — fct 전체 =
   N × U × 전체 dt. 검색 근거: DuckDB는 수십억 행·TB까지 단일노드로 처리하고, v1.5.3(2026-05)은
   jemalloc 정적 링크로 out-of-core 스필을 개선했다 — 300대×다년(수십억 행)이면 "빛나는 구간"
   상단 근처지만 아직 단일노드 영역
   ([단일노드 스케일링](https://iceberglakehouse.com/posts/2026-05-23-single-node-data-engineering-duckdb-datafusion-polars-lakesail/),
   [Big Data is Dead](https://motherduck.com/blog/big-data-is-dead/)). → **대응(측정 후)**:
   천장을 넘으면 순서대로 — (a) full-refresh를 dt 청크로 분할, (b) DuckDB
   client-server(원격 어태치)로 메모리 분리, (c) 그래도 넘으면 그때 Iceberg+분산 엔진
   (Trino/Spark). 프리미처 분산 금지.
4. **오케스트레이터 (거의 안 바뀐다)** — LocalExecutor는 머신 자원이 한계지만, 이 워크로드는
   태스크당 I/O 가벼운 **일 배치**다. 수백 개의 짧은 태스크는 `PARALLELISM` 상향으로 단일
   노드가 소화한다. 검색 근거: KubernetesExecutor는 수백 개의 짧은 태스크에 pod 생성·종료
   오버헤드로 오히려 나빠지고, Celery/K8s는 **HA·수천 동시 태스크·강한 격리**가 필요할 때의
   선택이다([Airflow executor 비교](https://www.astronomer.io/docs/learn/airflow-executors-explained)).
   → **안 바꾼다.** 전환 트리거만 명시: 단일 노드가 일 배치 창을 못 맞추거나 스케줄러 HA가
   요구될 때만 CeleryExecutor.

**PB 질문 — 이 데이터는 구조적으로 PB가 될 수 없다(숫자로)**: "고객이 대기업이면 몇백 TB~PB
아니냐"는 착각의 다른 형태다. 원천은 `query_snapshot` — **이미 집계된 쿼리 성능 메타데이터**이지
원본 로그·이벤트가 아니다. 10단계 실측(6대/년=54.5M행≈388MB, 행당 ~7바이트) 외삽: 500대×5년≈
**150GB**, 5,000대×10년(비현실적)≈**3TB** — 둘 다 단일노드 DuckDB 영역. PB 도달엔 ~15만
인스턴스-년이 필요해 이 데이터로는 불가능하다. **PB는 규모가 아니라 데이터 종류**(원본 로그·
클릭스트림·IoT)의 세계이고, DBTower가 일부러 메타데이터로 집계하므로 여기 도달하려면 파이프라인이
아니라 원천의 종류가 바뀌어야 한다 — 그건 별개 프로젝트다. 멀티엔진 동시성(Spark·Trino·Flink가
한 테이블 공유)도 "여러 팀·여러 엔진 동시"가 전제인데, 소비자가 Metabase 하나라 그 상황 자체가
없다.

**그래도 정말 커지면 — 아키텍처가 이미 탈출구(개방 포맷·dbt 추상화)를 열어놨다**: 데이터가
개방 포맷(Parquet)으로 S3에 있고 변환이 dbt로 추상화돼 있어, 만에 하나 벽을 넘으면 마이그레이션은
**엔진 교체**지 재설계가 아니다 — DuckLake→Iceberg(둘 다 Parquet+카탈로그), dbt-duckdb→
dbt-spark/trino(어댑터), 그리고 **데이터는 안 옮긴다**(이미 S3 개방 포맷). 이것이
"스토리지·컴퓨트 분리"를 택한 보상이다([Iceberg=멀티엔진 동시성 중재](https://www.starburst.io/blog/why-apache-iceberg-databricks-delta-lake/),
[컴퓨트-스토리지 분리](https://datalakehouse101.com/knowledge/decoupled-storage-compute.html)).
방침: **짓지 않되 이음새는 열어둔다**(개방 포맷·dbt·S3 유지 — 이미 그러함).

**두 번째 직교 축 — 멀티 DBTower(멀티테넌시)**: "몇백 대"가 *한 DBTower가 관제하는 인스턴스*가
아니라 *DBTower 배포 자체가 여러 개*(매니지드 운영자)라면, 그건 한 파이프라인을 키우는 문제가
아니라 **테넌트별 샤딩**(테넌트 = 자기 카탈로그 DB + 자기 S3 프리픽스 + 자기 DAG)이다. 이건
11단계의 "범용화 안 함" 판단과 같은 선에 있다 — 매니지드 SaaS로 갈 때의 이야기이고, 현
범위(단일 조직 셀프호스트)에선 밖. 필요해지면 `.env`의 카탈로그/프리픽스를 테넌트별로 분리한
스택 복제로 시작(공유 컴퓨트 아님).

### 착수 명세 (Opus) — 12단계 N축 실측

> **구현 담당: Opus.** 10단계의 격리 원칙 그대로 — 실데이터·원천 무접촉, 격리 프리픽스, 실측 후 정리.

| # | 조각 | 재활용 자산 | 구현 명세 | 검증 기준 |
|---|---|---|---|---|
| N1 | `scale_synthesize.py` 인스턴스축 모드 | `scripts/scale_synthesize.py`(dt축 복제 — `:118-128`이 인스턴스 원본 고정) | `--instances N` 옵션: 닫힌 dt parquet를 읽어 `instance_id`를 N개로 리매핑 복제(id 오프셋 + query_id에 인스턴스 suffix로 카디널리티 자연 증가). dt축(`--days`)과 직교 조합 가능. 격리 프리픽스 `scale_n/` | N=100 생성 후 파일 수 = N×dt, 원천·`raw/` 무접촉 |
| N2 | 측정 스크립트 | `scripts/scale_checkpoint_measure.py` | N=100/300 각각에서 측정: ① 파일 수·평균 크기(소파일) ② 글롭/플래닝 시간 ③ 게이트 per-dt ④ fct 증분 1-dt 시간 ⑤ fct full-refresh 시간 ⑥ CHECKPOINT 시간·전후 파일 수. 표로 출력 | "무너지는 순서" 표 확정 — 예상(소파일→추출→컴퓨트) 검증 or 반증 |
| N3 | 정리 | 10단계 정리 패턴 | `scale_n/` 프리픽스 삭제 + 카탈로그 테이블 DROP. 멱등 | 정리 후 잔존 오브젝트 0 |

**검증 기준(실측 TODO — 아직 미측정, 정직 표기)**: N=100/300을 격리 합성(10단계
`scale_synthesize`를 인스턴스축으로 확장, 실데이터·원천 무접촉) → 위 1~4의 **무너지는 순서와
수치**를 확정. 소파일이 먼저인지 추출이 먼저인지는 지금은 **외삽이지 실측이 아니다**(10단계가
시간축만 쟀으므로). 측정 전엔 "그럴 것이다"까지만 주장한다.

**잔여(정직 표기)**:
- **분산 컴퓨트(Spark/Trino)·per-tenant 샤딩은 수치가 요구할 때만** — 기존 "안 하기로 한 것"의
  Spark/Iceberg 판단 계승. 이 단계는 *언제* 요구되는지의 임계를 N축으로 정의할 뿐, 미리 짓지 않는다.
- **N축 실측 자체가 미완** — 이 단계는 분석·외삽·대응 설계까지다. 라이브 실측은 후속.

**산출물**: VERIFICATION 절(N축 규모표·무너지는 순서) · 블로그 "몇백 대를 관제하는 남 —
규모의 두 번째 축".

> 실행 기록(2026-07-17 라이브 실측 — VERIFICATION 13절): **N1~N3 완료. 그리고 위 외삽이
> 틀렸음을 실측이 밝혔다.** 설계는 "총량 고정·축 회전" — 10단계(365dt×6=2,190파일·54.5M행)와
> 등가인 **7dt×300inst=2,100파일·52.2M행**을 합성해 차이를 축의 모양으로 귀속시켰다. 결과:
> ① **일상 경로(증분)는 N=300에서도 건재** — 새 dt(300파일·7.5M행) 8.03s, 워터마크 프루닝이
> N축에서도 유효. ② **급소는 소파일이 아니라 full-refresh** — 같은 총량에서 dt축 407.62s vs
> N축 **769.21s(1.9배)**. dt당 행수가 149k→7.46M으로 커져 창 집계 압력이 집중되는 것이 가설
> (EXPLAIN 프로파일링 전까지 가설로 정직 표기). ③ 소파일(177KB 동일 프로파일)·글롭·게이트는
> 여전히 초 단위 이하. ④ 추출(원천 PG N인스턴스)은 합성으로 측정 불가 — 실원천의 몫으로 남김.
> 함의: 몇백 대 어플라이언스의 운영 가이드는 "증분은 그대로, **backfill/과거 정정(full-refresh)을
> dt 청크로 쪼개라**"가 1순위가 된다(12단계 대응 3(a)의 우선순위 상승).

---

## 13단계 — 용량 예측: DBTower가 구조적으로 못 보는 미래

**상황 가정**: 어플라이언스를 몇 달 돌렸다. DBA가 묻는다 — "이 인스턴스 디스크 언제 꽉 차?
다음 분기 증설 예산 얼마 잡아?" **DBTower는 답 못 한다** — 7일 시야라 증가율(GB/일)의 추세가
노이즈에 묻힌다. 이건 lakehouse의 존재 이유(장기 이력)와 정확히 겹치는 질문이다.

**한계 인지**: 용량 계획은 "지금 크기"가 아니라 "증가율 × 시간"이다. 며칠치로는 추세와
노이즈를 구분 못 한다. 그런데 정작 지금 lakehouse는 `query_snapshot`(쿼리 성능)만 아카이브하고
**크기 시계열은 안 담는다** — 예측할 재료 자체가 없다.

**경계 — 단기 ETA와 장기 추세는 다른 층이 맡는다 (DBTower 로드맵과 정렬, 2026-07-15)**:
시간 지평으로 역할이 갈린다. **단기(시간~며칠)** "24시간 내 디스크 참" 카나리아는 라이브 메트릭
층의 몫 — Prometheus `predict_linear` 패턴이 정석이고 DBTower 로드맵 Phase 5가 그 자리다(원천도
라이브 free-space 지표라 lakehouse가 낄 이유가 없다). **장기(주·월·분기 — 계절성·예산)**는
수개월 이력이 필요해 lakehouse만 할 수 있다 — 이 13단계가 그것이다. 같은 "용량 예측"이라도
두 레포가 겹치지 않는다: 지평이 다르고 원천이 다르다.

**판단 — DBTower가 낳고 lakehouse가 내다본다 (두 프로젝트 이음)**: 이건 새 레포(`dbtower-????`)가
아니라 **lakehouse의 새 mart 하나**(`mart_capacity_forecast`)다. 레포는 기능이 아니라 역할
(데이터 시야)로 가른다 — 용량 예측은 "장기 분석"이라 lakehouse의 집(프로젝트 경계는 아래
"dbtower 패밀리" 절). 원천 공급은 DBTower(이미 있는 `tableStats` 능력을 주기 스냅샷), 장기
아카이브·예측 본체는 lakehouse.

**개선(구현 계획)**:
- **계약 확장**: 원천에 크기 스냅샷(`size_snapshot`: instance_id·captured_at·object·bytes·rows)을
  세 번째 원천으로. `CONTRACT.md`에 `query_snapshot`·`database_instance`에 이어 추가.
  (DBTower 쪽은 `tableStats`를 주기 저장하는 작은 공급 — DBTower ROADMAP에도 반영 필요.)
- **파이프라인**: offload가 크기 스냅샷도 같은 파티션 규약(`dt=/instance_id=`)으로 내림 →
  staging → `fct_size_daily`(일별 크기·증분) → `mart_capacity_forecast`.
- **예측 모델(선형)**: 최근 N일 GB/일 기울기(최소자승) + 선택적 요일/월 계절 보정.
  "임계까지 잔여일 = (임계 − 현재) / 기울기". 기울기 표준오차로 낙관·비관 밴드(신뢰구간).
- **서빙**: Metabase 용량 대시보드 — 인스턴스별 증가율·임계 도달일. 지평 30/60/90일(운영
  경보)·12개월(예산). [운영은 30/60/90일이 실행 가능, 예산은 12개월이 실무 최소](https://www.jusdb.com/blog/database-capacity-planning-metrics-growth).
- **함정(웹 검증)**: 크기 카운터 리셋(truncate·재생성) → 음수 기울기 클램프(기존 델타 클램프
  재사용). 계단식 증가(파티션 추가)는 선형이 과대예측 → 최근 창 가중. 신규 인스턴스는
  데이터 부족 시 "학습 중" 표기(D1 베이스라인과 같은 정직 패턴).
- **임계(분모)의 세 원천 — "디스크 총량을 어떻게 아나"(웹 검증)**: 잔여일 계산엔 분자(사용량
  추세)만이 아니라 분모(한도)가 필요한데, 그 소스는 우선순위로 셋이다. ① **사용자 설정** —
  인스턴스별 quota/임계(GB). 온프레미스·정책 한도에 가장 정직하고 구현 0순위. ② **원천 DB가
  스스로 아는 것(기종별 정직)** — MSSQL은 `sys.dm_os_volume_stats`로 볼륨 total/available을
  SQL로 직접 보고([MS 공식](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-volume-stats-transact-sql?view=azuresqldb-current)),
  Oracle은 `dba_data_files.maxbytes`(autoextend 상한)를 안다. **PG·MySQL·Mongo는 DB 쿼리로
  볼륨 총량을 못 본다 → UNSUPPORTED 정직 표기**(DBTower 원칙 승계). ③ **메트릭 층 연동(선택)** —
  볼륨 총량의 정석 소스는 node_exporter/CloudWatch이고 DBTower 직무 지도가 이미 "OS/디스크
  시계열은 메트릭 층 위임"으로 선을 그었다 — 재발명하지 않고 선택 연동까지만. **임계가 없어도
  증가율(GB/일)+추세 급변 감지는 그 자체로 산출물이다**(D-day만 임계 필요).
- **오토스케일 환경 고려 — "현업은 자동 증설인데 의미 있나"(웹 검증)**: 있다, 소비자가 바뀔
  뿐이다. (a) 오토스케일도 한도가 있다 — RDS storage autoscaling은 **max storage threshold**
  이상 못 크고, 증설 후 **6시간 쿨다운** 동안 대량 적재 시 storage-full에 갇힐 수 있다고 AWS가
  공식 경고([RDS autoscaling](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PIOPS.Autoscaling.html)).
  임계는 소멸이 아니라 이동. (b) K8s PVC 확장은 자동이 아니다 — `allowVolumeExpansion` + PVC
  편집은 사람/오퍼레이터의 행위([K8s 공식](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)),
  예측은 "언제 편집할지"의 신호. (c) 온프레미스 IDC는 증설 = 구매·리드타임 몇 주~몇 달 —
  예측이 가장 중요한 환경. (d) 오토스케일이 흡수하는 환경에선 소비자가 장애 방지 →
  **비용 계획(FinOps)**으로 바뀐다 — 로그 폭주를 조용히 흡수하면 비용 폭탄이므로 성장률 이상
  감지가 잡는다. **자동 증설 실행(테라폼/PVC patch)은 안 한다** — DBTower가 자동 페일오버를
  안 하는 그 원칙("바꾸는 주체가 따로 있는 일은 그 주체와 잇는다"). 예측·경보까지가 우리 몫.
- **알림의 의미는 환경이 아니라 "임계의 종류"가 정한다 (설계 귀결, 2026-07-15)**: 오토스케일은
  임계를 없애는 게 아니라 **물리 → 설정값 → 돈**으로 옮길 뿐이다. 환경별로 같은 예측이 다른
  알림이 된다:

  | 환경 | 임계의 실체 | 알림의 의미 |
  |---|---|---|
  | 온프레미스/IDC | 물리 디스크 | "포화 D-day" — 증설 리드타임 몇 주~몇 달이라 생명줄 |
  | RDS 오토스케일 | max storage threshold(사용자 상한) | "상한 도달 D-day" — 상한 닿으면 결국 수동 개입 |
  | Aurora급(사실상 무한) | 예산 | "이 성장률이면 N개월 뒤 비용 X" — 순수 FinOps |

  도구가 환경을 감지할 필요가 없다 — **임계 1순위가 "사용자 설정"인 이유가 바로 이것**이다.
  사용자가 넣는 값(물리 2TB / RDS 상한 5TB / 임계 없음=증가율·비용만)이 환경의 의미를 함께
  주입하므로, 구현은 **임계 종류별로 판정·문구 컬럼만 갈라주면** 된다("꽉 차요" / "상한
  도달해요" / "비용 이만큼 늘어요"). AWS도 오토스케일과 별개로 경보를 카나리아로 두라고
  권고한다 — 갑작스런 성장은 "왜 크는지 조사하라"는 신호이고, 오토스케일이 그걸 조용히
  흡수하면 장애 대신 비용 폭탄이 온다(위 (d)와 같은 근거).
- **발화 주체 — lakehouse는 데이터 알림을 직접 쏘지 않는다 (2026-07-15 확정)**: lakehouse의
  알림은 **자기 감시 3종**(태스크 실패·게이트 FAIL·deadman — "파이프라인이 아프다")까지가
  전부이고, 이는 이미 구현돼 있다. "데이터가 말한다"류(용량 D-day·추세 급변)는 성격이 다르다 —
  D-day 몇 개월짜리는 알림이 아니라 **리포트**(1차 소비처 = Metabase 대시보드, pull)이고,
  정말 push가 필요한 판정(D-30 진입·성장률 급변)의 발화는 **관제탑(DBTower)의 직무**다. 알림
  인프라(웹훅·쿨다운·레이트리밋·심각도)는 DBTower가 이미 소유하며, lakehouse가 직접 쏘면 알림
  시스템이 이원화된다. 경로는 DBTower 로드맵 Phase 5의 reverse ETL — "되읽기는 기계가 소비해
  액션을 구동할 때만 정당" 원칙의 정확한 사례("보기는 Metabase, 발화는 DBTower"). **13단계
  mart의 역할은 판정 컬럼(risk kind·D-day·급변 플래그) 계산까지다.**

**왜 ML이 아니라 선형회귀인가 (범위 정직)**: 용량 예측 상용툴(Redgate·SolarWinds·Oracle Ops
Insights)도 선형·단순 통계 외삽이 기본이다([SolarWinds 디스크 포캐스트](https://www.solarwinds.com/sql-sentry/use-cases/database-storage-forecasting-tool),
[Oracle Ops Insights capacity](https://docs.oracle.com/en-us/iaas/operations-insights/doc/database-capacity-planning.html)).
GB/일은 가장 예측 가능한 메트릭이라 선형+계절 보정으로 충분하고, ARIMA/Prophet/LSTM은 이
규모·이 신호에 과잉이다 — DBTower 이상감지(D1)가 z-score이지 ML이 아닌 것과 같은 판단.
"못 해서 안 하는 게 아니라 필요 없어서 안 한다"를 수치로 소명한다.

### 착수 명세 (Opus) — 13단계 용량 예측

> **구현 담당: Opus.** 전제: DBTower 쪽 `size_snapshot` 공급(tableStats 주기 저장)이 먼저다 —
> 그 전엔 C1(계약)과 C5(합성 검증)까지만 가능. 기존 파이프라인 패턴(offload→staging→fct→mart→
> 게이트·발행) 최대 재사용, 새 축 최소화.

| # | 조각 | 재활용 자산 | 구현 명세 | 검증 기준 |
|---|---|---|---|---|
| C1 | 계약 확장 | `docs/CONTRACT.md` §1·§8 | `size_snapshot` 원천 스키마 확정·정식 편입: `id, instance_id, captured_at, object_type(db\|table), object_name, data_bytes, index_bytes, row_estimate` + **기종별로 아는 것만** `volume_total_bytes, volume_available_bytes, max_bytes`(MSSQL 볼륨·Oracle maxbytes — 모르면 NULL, 임계 원천 ②). §8 비계약에서 본계약으로 이동. **연동 노트**: DBTower에 node_exporter가 붙으면(그쪽 Phase 5 예정) volume 필드를 **전 기종**에 채울 수 있다 — node_exporter는 엔진 무관 파일시스템 지표라 PG/MySQL/Mongo의 UNSUPPORTED를 우회(단 호스트 접근 가능한 온프레미스/VM만 — RDS류는 CloudWatch 영역이라 여전히 사용자 seed). 스키마는 이 가능성을 전제로 volume 컬럼을 nullable로 잡아둔다 | CONTRACT 갱신 + DBTower 쪽 동일 스키마 합의 기록 |
| C2 | 추출 확장 | `extract/offload.py`(인스턴스 루프·멱등·자기파괴 가드) | `raw/size_snapshot/dt=/instance_id=/` 동일 파티션 규약으로 두 번째 오프로드. 가드·게이트(정합·완결성) 승계. parquet 스키마 명시 선언(C1) | 멱등 2회 행수 불변, 게이트 통과 |
| C3 | dbt 모델 | `stg_query_snapshot`·`fct_query_daily` 패턴, contracts | `stg_size_snapshot`(dedup) → `fct_size_daily`(일별 last값, 증분 delete+insert, dt 워터마크 — fct_query_daily와 동일 전략) → `mart_capacity_forecast`: 최근 N일(기본 30) `regr_slope(bytes, epoch_day)`(DuckDB 내장) 기울기 + 표준오차 밴드, 음수 기울기 클램프(리셋), 관측 < 14일이면 `learning=true`. contract enforced | unit test: 선형 성장 기울기 정확, 리셋 클램프, 학습중 |
| C4 | 임계 주입(사용자 설정 = 원천 ①) | dbt seeds | `seeds/capacity_thresholds.csv`: `instance_id, threshold_bytes, threshold_kind(physical\|autoscale_max\|budget\|none)`. mart가 left join해 kind별 **판정 컬럼** 산출: `physical/autoscale_max` → 잔여일 D-day, `budget/none` → 증가율·비용 추정만. 문구·급변 플래그도 컬럼로만(위 "알림의 의미" 표). **발화는 안 한다** — 소비는 Metabase(사람) 또는 DBTower reverse ETL(기계, 위 "발화 주체") | seed 교체만으로 판정 의미가 바뀌는 것 확인 |
| C5 | 합성 검증 | 10단계 합성 패턴 | 합성 성장 주입 스크립트: 선형(+기울기 기지값)·주간 계절·중간 리셋 1회 → mart 기울기가 기지값 ±오차 내, 잔여일 산식 검증. 원천 무접촉 | 기지값 대비 오차율 기록 → VERIFICATION |
| C6 | 서빙 | `scripts/metabase_bootstrap.py` 패턴 | Metabase 용량 대시보드: 인스턴스별 증가율(GB/일)·D-day(임계 있는 것만)·추세 급변 플래그. 운영 대시보드(10단계)와 별도 카드 | 화면 실측 스크린샷 |

**함정(선검증)**: DuckDB `regr_slope`는 NULL 쌍 제외 — 결측일은 보간하지 말고 그대로(관측
기반). 계단식 성장(파티션 일괄 추가)은 최근 창 가중이 아니라 **창 내 구간 분할 감지**가 정답일
수 있으나 1차는 단순 최근 N일 창으로(과설계 금지, 실측이 요구하면 개선). `epoch_day`는 dt
기반(타임존 무관).

**검증 기준(실측 TODO)**: 합성 증가 데이터(선형+계절+리셋 주입)로 기울기·잔여일 정확, 리셋
클램프·계단 대응·데이터 부족 표기 라이브. 실데이터로 증가율 대시보드.

> 실행 기록(2026-07-18 라이브 실측 — VERIFICATION 15절): **C1~C5 완료, C6(Metabase 대시보드)은
> 이력이 쌓이면.** DBTower가 V26 `size_snapshot` + SizeSnapshotJob(6시간 주기·7일 보존)을 낳아
> (그쪽 102절) 공급이 열렸다 — 첫 사이클 6인스턴스 43오브젝트 실측. lakehouse: 레지스트리
> 편입(43행 멱등·게이트 통과), `stg_size_snapshot`→`fct_size_daily`(하루 대푯값=마지막 관측,
> 증분 워터마크 패턴)→`mart_capacity_forecast`(regr_slope·판정 컬럼·발화 없음) +
> `seeds/capacity_thresholds`(임계 원천 ①, kind별 의미). **산식은 unit test 기지값으로 고정**:
> 선형 성장 20일(+10MB/일, 임계 2000MB) → slope=10MB/일·잔여 81일·d90 정확, 역성장 →
> days NULL·stable_or_shrinking(지어내지 않음). 실데이터는 이력 1dt라 전 인스턴스
> learning=True — 정직한 초기 상태(이력이 쌓이면 찬다). publish 6마트 편입, CI 픽스처에
> size 소스 추가(로컬 재현 PASS=44). volume_*(임계 원천 ②)은 수집기 미공급(NULL) — 후속.

**잔여**: 정교한 시계열 모델(ARIMA/Prophet)은 선형이 실측으로 부족함을 확인하면. IOPS·CPU 등
크기 외 자원 예측은 별개 축. lakehouse 자체 AI/MCP는 안 함(아래 "안 하기로 한 것").

**산출물**: VERIFICATION 절 · CONTRACT 세 번째 원천 · 블로그 "DBTower가 못 보는 미래 — 용량 예측".

---

## 셀프호스트·규모 종합 판정 (11·12단계 요약 — 2026-07-14 결정)

> "이걸 남들도 셀프호스트로 쓰게 하고 싶다. 근데 셀프호스터가 몇천 대 DB를 가진
> 대기업이면?"에서 출발한 판단을 한곳에 종합한다. 각 판정의 근거는 11·12단계 본문과
> 웹 리서치(각 항목 URL). 결론부터: **어플라이언스 한 대로 몇천 대를 버틴다. 분산 재설계는
> 필요 없다. 노브 두 개만 노출한다.**
>
> **읽는 법 (중복 방지)**: 이 문서의 셀프호스트·규모 논의는 세 역할로 나뉜다 — **11~13단계**는
> *실행 아크*(상황→개선→검증), **이 절**은 *결정 요약(SoT)*(정체성·규모·분산·용량의 판정을
> 한곳에), **아래 "dbtower 패밀리" 절**은 *레포 경계*(무엇이 dbtower냐 lakehouse냐). 정체성
> 근거는 여기 §1이 원본이고, 다른 곳(11단계·패밀리)은 그걸 참조한다.

### 1. 제품 정체성 — DBTower의 장기 분석 애드온(어플라이언스), 범용 도구 아님

- **비유**: DBTower가 Prometheus면 이건 **Thanos**다 — 단기 관제(DBTower, 7일)와 장기 이력
  (lakehouse)을 잇는 애드온. 사용자 = *아무나*가 아니라 **DBTower를 셀프호스트하는 사람**.
- **범용화(아무 DB에 붙는 쿼리 분석 도구)는 안 한다** — PMM·pgwatch·OpenObserve 레드오션이고,
  DBTower 결합(관측 원천을 직접 소유)이라는 유일한 차별점이 죽는다. 11단계 판단.
- **형태 = 어플라이언스(배터리 포함 상자)**: DuckDB/DuckLake/Airflow/Metabase는 상자 안에 숨고,
  사용자는 `.env`에 자기 DBTower만 적고 `docker compose up` → Metabase만 본다. "DuckDB를 굴리기
  싫다"는 걱정은 성립 안 한다 — 안 보이기 때문(Grafana의 저장엔진, GitLab의 Redis와 같다).

### 2. 현업 정합성 — 이 구조는 즉흥이 아니라 업계가 수렴한 정답의 축소판

- **운영계(OLTP)↔분석계(OLAP) 분리**: 데이터 아키텍처 1번 원칙(Kimball/Inmon). DBTower(관제)
  vs lakehouse(분석)가 정확히 이것.
- **관측 도구의 단기→장기 오프로드**: Prometheus→Thanos/Cortex/Mimir, Elastic hot-warm-cold,
  Datadog archive rehydration과 같은 표준 패턴.
- **메달리온(bronze/silver/gold)**: raw→staging→marts가 이 정식 구조 그대로.
- **"하나의 lakehouse에 다 넣나?"**: 분석계 안에서는 **그렇다**(DuckLake 하나가 bronze/silver/gold를
  다 품음 = lakehouse의 정의). 단 운영계(DBTower)는 그 하나에 안 들어간다 — 엔진(밀리초 조회 vs
  초 단위 스캔)이 달라서. 즉 **운영↔분석은 분리, 분석 내부는 통합**이 2026 정석 모양이고 둘 다
  하고 있다.
- **축소판인가**: 구조는 대형 패턴의 충실한 축소판(Spark→DuckDB, Iceberg→DuckLake는 개념 1:1,
  전환은 어댑터 문제). 데이터 층에선 "축소판"이 아니라 **이 데이터에 딱 맞는 정품**(아래 4·5).

### 3. 규모 판정(N축) — 몇천 대여도 데이터는 low-TB, 단일노드 유지

12단계 표 요약(인스턴스-년당 365파일·9.08M행 외삽):

| N | 연 행수 | 크기/년 | 판정 |
|---|---|---|---|
| 300 | ~2.7B | ~19GB | 단일노드 여유 |
| 3,000 (몇천 대 대기업) | ~27B | ~190GB | **단일노드 유지**, 노브 튜닝 |
| 5,000 × 10년 | ~450B | ~3TB | 여전히 단일 DuckDB |

- **PB는 불가능**: 원천이 이미 집계된 쿼리 메타데이터라 ~15만 인스턴스-년이 필요. PB는 규모가
  아니라 **데이터 종류**(원본 로그·이벤트)의 세계 — 도달하려면 원천이 바뀌어야 하고 그건 별개 프로젝트.

### 4. "깨지나 / 분산해야 하나" — 데이터·속도 어느 쪽으로도 NO

- **크래시 깨짐(데이터가 노드를 넘어 OOM)**: 안 일어난다(low-TB, 넘으면 디스크 스필).
- **소프트 깨짐(느려짐)**: 딱 둘 — ①소파일 폭증(→컴팩션 노브) ②추출이 배치 창 초과(→병렬도 노브,
  단 메타 PG 부하 상한이 천장 = 가드레일 1). 튜닝이지 재설계 아님.
- **쿼리 속도로도 분산 NO**: 대시보드는 수십억 행(raw/fct)이 아니라 **사전집계된 gold 마트**를
  읽는다(메달리온의 존재 이유 — 마트는 365dt에도 0.31s 실측). raw가 커도 대시보드 속도는 무관.
  fct 애드혹도 파티션 프루닝+컬럼나로 필터 쿼리는 초 단위. 유일 예외 = fct 전체 무필터 통스캔인데
  대시보드 워크로드가 아니다(정직 표기).
- **"분산"의 두 뜻 구분**: 규모 분산(데이터가 커서) = 불필요. HA 분산(노드 죽어도 계속) = 규모가
  아니라 안정성 문제이고, 일 배치라 SLA가 느슨해(죽으면 재실행, deadman이 침묵 감지) **optional**.

### 5. 용량이 작은 이유 — lakehouse여서가 아니라 담는 데이터가 달라서

- 대형 lakehouse가 PB를 먹는 건 **원본 이벤트 firehose**(클릭·로그·센서·JSON, 나중에 분석하려고
  원본을 못 버림)를 담아서다. 이 프로젝트는 **DBTower가 원천에서 이미 집계한 메타데이터**(좁은
  8컬럼·행당 ~7바이트)를 담는다 — firehose가 아니라 요약본.
- **핵심**: "원천에서 일찍 집계한다"는 설계 선택이 용량을 결정했다. 같은 도구(DuckLake)로도 쿼리
  실행 하나하나를 raw 이벤트로 담았다면 TB~PB가 돼 분산이 필요했을 것. **같은 도구, 다른 데이터 =
  다른 규모.** "Big Data is Dead"의 그 지점([근거](https://motherduck.com/blog/big-data-is-dead/)).

### 6. 만에 하나 정말 커지면 — 짓지 않되 이음새는 열어둠

데이터가 개방 포맷(Parquet)으로 S3에, 변환이 dbt로 추상화돼 있어 마이그레이션은 **엔진 교체**지
재설계가 아니다: DuckLake→Iceberg, dbt-duckdb→dbt-spark/trino, **데이터는 안 옮김**(이미 S3
개방 포맷). 이것이 스토리지·컴퓨트 분리의 보상. 방침: 짓지 않고 이음새만 유지(이미 그러함).

### 7. 종합 결론 및 착수 범위

**몇천 대 대기업이 셀프호스트해도 어플라이언스 한 대로 버틴다. 재설계 0. 노브 두 개(컴팩션
스케줄·추출 병렬도)를 설정값으로 노출하면 끝.** 실제 착수 작업은 11단계 "개선(구현 계획)" 5축:
결합 분리(진행 — `config.py` 카탈로그↔원천 분리 완료, `CONTRACT.md` 두 테이블 정정 완료) →
시크릿 위생 → 독립 실행(standalone compose + 노브 노출) → 노출·인증·TLS → 릴리스·설치 계약.

---

## 14단계 — 두 저장소가 손잡기: offload 확대 + 장기 베이스라인 되쓰기 (완료 — 실행 기록 2026-07-18, VERIFICATION 14절 + DBTower 100~101절)

**상황 가정**: DBTower의 이상 감지 베이스라인은 7일 창(요일x시간대)이다. 월요일 아침 배치 피크가
매주 반복되는 인스턴스에서, 4주 전 같은 요일과 비교하면 평범한 부하를 "평소와 다름"으로 오탐한다.
그리고 offload는 여전히 `query_snapshot` 하나뿐 — wait event·백업 이력·플랜 변경의 장기 추세는
7일 뒤 사라진다(이 프로젝트가 존재하는 이유였던 그 구멍이 세 테이블에 아직 남아 있다).

**한계·전제 (2026-07-16 DBTower 쪽 실분석 — 원문은 DBTower ROADMAP Phase 5 표, 지어낸 것 없음)**:
"코드 변경 0으로 추출만 늘리면 된다"는 전제가 실분석에서 셋 다 깨졌다.

- **선결 1 — wait event는 원천에 영속 테이블이 없다**: DBTower 메타 DB에 wait event 주기 영속이
  없어 추출할 것 자체가 없다. `wait_event_snapshot(instance_id, captured_at, event_name, category,
  value)` 신설이 먼저다(13단계 size_snapshot과 동일 패턴 — **DBTower 쪽 작업**).
- **선결 2 — plan_snapshot 보존이 카운트 기반**(쿼리당 최신 20개, 1시간 스윕): "어제 하루창 추출"
  전제와 어긋나 하루가 닫히기 전에 행이 지워질 수 있다 → 시간 기반 보존 병행 또는 추출 주기
  단축의 정합 문서화(**DBTower 쪽 작업**).
- **선결 3 — backup_run은 사후 변이 테이블**(verify/remote가 나중에 UPDATE): "닫힌 dt는 불변"
  전제가 깨진다 → D+1 스냅샷임을 계약에 명시, 워터마크 컬럼은 started_at.

**판단**: forward(내리기)와 reverse(되쓰기)는 방향이 다른 두 일이다. forward는 기존 파이프라인의
일반화(단수 상수 → 레지스트리), reverse는 **원천 readonly 봉인을 깨지 않는 별도 쓰기 경로**의
신설이다 — 섞으면 "분석계가 운영계를 오염시키지 않는다"는 이 프로젝트의 안전 논거가 무너진다.
되쓰기는 "기계가 소비해 액션을 구동할 때만 정당" 원칙의 두 번째 사례다(첫째는 13단계 용량 발화).

### 착수 명세 (Opus) — 14단계

> 선결(D1·D2)과 수신(D8)은 DBTower 저장소 작업, 나머지는 이 저장소. 기존 자산(offload 멱등·게이트·
> 증분 워터마크·publish 원자성) 최대 재사용.

| # | 조각 | 어디 집 | 구현 명세 | 검증 기준 |
|---|---|---|---|---|
| D1 | wait_event 주기 영속 | DBTower | `wait_event_snapshot` 테이블 + 수집 잡 + 보존(7일, 기존 retention 패턴). 이게 없으면 D3의 wait event 추출은 공급원이 없다 | 주기 적재 실측, 보존 정리 동작 |
| D2 | plan_snapshot 보존 정합 | DBTower | 시간 기반 보존 병행(최소 48h 보장) 또는 "카운트 보존 + 추출 주기 단축" 계약 문서화 중 택1 — 하루창이 닫히기 전 유실 없음을 보장 | 경계 케이스(20개 초과 갱신 쿼리)에서 어제 행 생존 |
| D3 | offload 레지스트리화 | lakehouse | `SOURCE_TABLE` 단수 상수 → 테이블 스펙 레지스트리(테이블명·워터마크 컬럼·불변성 종류·파티션 규약). backup_run(D+1·started_at 워터마크)·plan_snapshot·wait_event_snapshot 편입. CONTRACT §1 개정 + GRANT 목록 추가 — 약 10개 파일 동심원 | 테이블별 멱등 2회 행수 불변 |
| D4 | 게이트 프로필 | lakehouse | 게이트 4축(정합·완결·신선도·볼륨)을 테이블별 프로필로 — backup_run은 저빈도·사후 변이라 completeness·freshness를 query_snapshot 기준으로 재면 **정상 상태가 fail-closed 오탐** | backup_run 무변경 날 게이트 PASS |
| D5 | fct_query_hourly | lakehouse | 일간 마트로는 dow×hour 통계가 불가 — staging에서 시간대별 델타 fct 신설(증분 delete+insert·dt 워터마크 리터럴 패턴 복제) | unit: 시간 경계 델타 정확 |
| D6 | 장기 베이스라인 mart | lakehouse | `mart_baseline_longterm(instance_id, query_id, dow, hour, mean, stddev, observations, computed_at)` — min_observations 필터(DBTower BaselineService 8관측 게이트와 정렬) + top-K 볼륨 가드(instance×query×168버킷 폭발 방지) | 합성 계절 데이터로 dow×hour 통계 기지값 일치 |
| D7 | 되쓰기(writeback) | lakehouse | 별도 역할 `lakehouse_writer`(해당 테이블만 INSERT/DELETE) + 별도 WritebackConfig(**SourceConfig 재사용 금지** — 원천 접속은 계약·코드 양쪽 readonly 봉인 유지). 스케줄은 publish~heartbeat 사이 writeback 태스크(단일 트랜잭션 DELETE+INSERT, publish의 원자성·행수 대조 이식) 또는 @weekly 별도 DAG(deadman 편입 조건). CONTRACT에 되쓰기 절 신설 | 원천 계정으로 쓰기 시도 → 거부, writer로 왕복 행수 대조 |
| D8 | 수신·병합 | DBTower | Flyway로 `baseline_longterm` 정의 + BaselineService 가중 병합(관측 충분 시 결합, 미존재/빈 테이블이면 현행 그대로 — 회귀 0) | 장기 테이블 주입 후 월요일 피크를 오탐하지 않음 실측, 미존재 시 현행 동작 회귀 없음 |

**함정(선검증)**: (a) 되쓰기 DELETE+INSERT가 DBTower의 이상 감지 폴러와 경합하면 빈 테이블을
읽는 순간이 생긴다 — 단일 트랜잭션이면 PG MVCC가 막아주지만, "트랜잭션 하나"가 계약임을 테스트로
고정할 것. (b) dow×hour의 타임존 — 원천이 UTC 고정이므로 mart도 UTC로 계산하고 표시만 로컬
(DBTower Slow 로컬 시간 표시와 같은 결). (c) top-K에서 잘린 쿼리의 베이스라인 부재는 "장기 없음 →
현행 7일 창 폴백"이지 오류가 아니다(BaselineService 병합 규칙에 명시).

**검증 기준(실측 TODO)**: DAG e2e(테이블별 게이트 프로필 통과) + plan_snapshot 보존 정합 실측 +
계절성 오탐 시나리오(합성 월요일 피크 4주 주입 → 5주차 월요일 무경보) + 되쓰기 왕복.

> 실행 기록(2026-07-18 라이브 실측 — VERIFICATION 14절): **lakehouse 몫(D3~D7) 구현·검증 완료.**
> D3 `extract/tables.py` 레지스트리(+offload 일반화, 하위호환·57 pytest 회귀 0) — 실원천에서
> backup_run 24행·plan_snapshot 13행 멱등 추출, wait_event는 시끄러운 거부(D1 대기). D4 게이트
> 프로필 — completeness·freshness SKIP이 실데이터(3/6 인스턴스만 백업)에서 오탐을 막는 것 실증.
> D5 `fct_query_hourly`(32,498행) · D6 `mart_baseline_longterm`(contract enforced, 기본 0행 =
> 이력 6dt의 정직한 결과, min_obs=1 검증으로 로직 확인). D7 되쓰기 — **32,498행 단일 트랜잭션
> 왕복 + 권한 격리 실증**(writer로 query_snapshot SELECT → permission denied) + no-op. DAG에
> aux 브랜치·writeback 병렬 배선. **남은 것: D1·D2·D8(DBTower 저장소) + 계절성 오탐 시나리오
> (4주 이력이 쌓이거나 합성 주입 시)** — 그 전까지 베이스라인 mart는 정직하게 빈다.

---

## 15단계 — 자연어 서빙: "지난 분기 대비 느려진 쿼리 보여줘"가 차트가 되기까지 (N1~N3 완료 — 실행 기록 2026-07-18, DBTower 103절 · N4는 드라이버 대기)

**상황 가정**: 대시보드는 만든 질문에만 답한다. 사용자가 "지난 분기 대비 느려진 쿼리를
인스턴스별로 보여줘"라고 **말로** 물으면, 지금은 누군가 Metabase에서 카드를 손으로 만들어야
한다. Metabase의 AI(Metabot)를 켜면 되지 않나?

**한계 인지 — Metabot은 셀프호스트에 없다 (2026-07-17 웹 검증)**: Metabot은 Cloud 전용 유료
애드온($100/월, 500요청)이고 **셀프호스트 OSS엔 제공되지 않는다**
([Metabot 문서](https://www.metabase.com/docs/latest/ai/metabot)). Metabase 60이 공식 MCP 서버
(`/api/metabase-mcp` — 테이블 검색·대시보드 생성·질문 저장·SQL 실행)를 실었지만
([발표](https://dev.to/metabase/metabase-60-we-made-ai-open-source-official-mcp-server-metabot-in-slack-split-panel-charts-and-3eb0)),
우리 DuckDB 드라이버(MotherDuck)는 **Metabase 59까지만 확인** — 드라이버-Metabase 버전 짝
고정(7단계 함정)이라 60 업그레이드는 드라이버가 따라올 때까지 보류
([드라이버 릴리스](https://github.com/motherduckdb/metabase_duckdb_driver)).

**판단 — 부품 재조립이 정답. 대화는 에이전트에서, 결과물이 Metabase에 생긴다**: 필요한 부품
셋이 이미 있다. (1) DBTower MCP 서버 + OAuth 2.1 + 도구 14종(완성), (2) DuckLake read-only
접속(Metabase가 쓰는 그 경로), (3) **Metabase REST로 카드·대시보드를 만드는 코드 —
`scripts/metabase_bootstrap.py`가 이미 하는 일**. 이 셋을 이으면: 자연어 → 에이전트 →
DBTower MCP(mart 조회 + 카드 생성) → Metabase에 차트 생성 → URL 반환. Metabot과의 차이:
Metabot은 Metabase 안 데이터만 보지만, 이 경로는 **장기 mart 조회 + 라이브 진단(기존 14도구) +
차트 생성**을 한 대화에서 섞는다("느려진 쿼리 찾고 → EXPLAIN 하고 → 대시보드로 박아줘").
DBTower Discord 봇 인바운드와 조합하면 "Discord에서 물으면 Metabase 차트 링크가 답글로"까지 —
Metabot이 못 하는 그림. 경계 원칙 유지: **lakehouse는 자체 AI/MCP를 갖지 않는다**("안 하기로
한 것" 참조) — 이 단계에서 lakehouse의 몫은 mart 접근·부트스트랩 패턴 제공까지고, MCP 도구
구현은 DBTower 집이다.

### 착수 명세 — 15단계

| # | 조각 | 어디 집 | 구현 명세 | 검증 기준 |
|---|---|---|---|---|
| N1 | mart 조회 MCP 도구 | DBTower | DuckLake read-only 접속(DuckDB JDBC — 카탈로그 PG DSN + S3 자격증명, Metabase 커넥션과 동일 파라미터)으로 `mart_query_regression`·`pipeline_run_log`(추후 13·14단계 mart 자동 편입) 조회 도구. **read-only 세션 강제**(원천 무오염 원칙의 분석계판) + 결과 행수 상한 | MCP로 장기 랭킹 질의 왕복, 쓰기 시도 거부 |
| N2 | 카드/대시보드 생성 MCP 도구 | DBTower | `metabase_bootstrap.py`의 REST 패턴(세션 → 질문 생성 → 대시보드 배선)을 도구화 — 입력: SQL+차트 타입+제목, 출력: 카드 URL. Metabase API 키(v0.48+) 사용, 생성은 전용 컬렉션에 격리(사람 대시보드 오염 방지) | 자연어 → 카드 생성 → URL 접속 시 차트 표시 e2e |
| N3 | 시스템 프롬프트·판단 기준 | DBTower | mart 스키마·컬럼 의미(dbt description 재사용)를 도구 설명에 — 에이전트가 지어내지 않고 실재 컬럼으로 SQL을 짜게. ai-analysis-rules 패턴("근거 없으면 모른다") 승계 | 존재하지 않는 컬럼 질의 시 거부/정정 |
| N4 | (보류) Metabase 60 공식 MCP | — | DuckDB 드라이버가 60 지원을 릴리스하면 재검토 — 그때는 N2를 공식 MCP로 대체 가능한지 비교 | 드라이버 릴리스 노트 확인 |

> 실행 기록(2026-07-18): **N1~N3 구현 완료 — DBTower 저장소에서**(그쪽 VERIFICATION 103절,
> 커밋 cdcd9f0). 설계 변경 1건: DuckDB JDBC 직접 의존 대신 **Metabase API 경유**(서빙 계층
> 재사용 — 의존 0, "Metabase는 DuckLake만 read-only" 계약과 정합). `lakehouse_query`(SELECT
> 가드·실재 스키마를 도구 설명에)·`lakehouse_card_create`("DBTower AI" 컬렉션 격리) — 라이브:
> mart_capacity_forecast 질의 6행, DELETE 400 거부, **카드 76 생성·bar 차트 실화면**. N4(Metabase
> 60 공식 MCP)는 드라이버 대기 그대로.

---

## 16단계 — 판정의 마지막 마일: 플랜 회귀·백업 공백·주간 보고 (구현 완료 — 2026-07-18, VERIFICATION 17절)

**상황 가정 셋 (전부 "사람 병목" — 현업 DBA의 시간이 어디서 새는가)**:

1. **새벽 3시, 특정 쿼리가 갑자기 느려졌다는 신고.** 단골 원인은 옵티마이저의 플랜 변경 —
   통계 갱신·데이터 분포 변화로 플랜을 갈아탔는데 새 플랜이 더 느린 경우다. 지금 창고에는
   `plan_snapshot`(플랜이 언제 어떤 해시로 바뀌었나)과 `fct_query_daily`(그 쿼리의 일별
   평균 지연)가 **둘 다 있는데 서로 모른다**. DBA는 여전히 플랜 이력과 지연 그래프를
   눈으로 대조한다(건당 30분). 이건 "며칠치 전후 비교"가 필요한 판정이라 라이브 7일
   시야(DBTower)가 아니라 장기 창고의 몫이다.
2. **복구하려고 보니 백업이 3주째 조용히 안 돌고 있었다.** DBA 최악의 시나리오. `fct_backup_daily`는
   일별 성공/실패 집계까지만 있고 **"이 인스턴스의 마지막 성공 백업이 며칠 전인가"라는 판정
   컬럼이 없다**. 실패는 시끄럽지만(FAILED 행이 남음) **공백은 조용하다**(행 자체가 없음) —
   지금 실데이터도 6인스턴스 중 3개만 백업 기록이 있는데, 그 3개의 부재를 아무도 묻지 않는다.
3. **월요일 오전은 보고서 만드는 날.** 현업 DBA 시간을 제일 많이 먹는 건 장애가 아니라 보고다.
   용량 D-day·대기 이벤트 top·플랜 변경 건수·백업 상태를 매주 사람이 화면 4곳에서 긁어모아
   문서로 만든다. 재료(마트 4개)는 전부 창고에 있는데 **한 장으로 접는 계층이 없다**.

**한계·전제 (2026-07-18 실데이터 기준 — 지어낸 것 없음)**:

- `fct_plan_change_daily`는 (instance_id, dt) grain의 **건수 집계**라 회귀 판정 재료가 못 된다.
  판정은 쿼리 단위 뒤집힘 **이벤트**(어느 쿼리가 언제 어떤 해시→해시로)가 필요하고, 그건
  `stg_plan_snapshot`(id, instance_id, query_id, plan_hash, captured_at, dt)에서 다시 꺼내야 한다.
- plan_snapshot 원천 보존은 카운트 기반(쿼리당 최신 20개) + 48h 하한(D2)이다. 창고에 적재된
  뒤로는 영구지만, **적재 이전 과거의 "직전 플랜"은 이미 지워졌을 수 있다** — lag 기반 뒤집힘
  검출은 창고에 남은 이력 기준이고, D2(2026-07-18) 이후 적재분부터 정확하다. 정직하게 명기.
- 백업 공백의 "기준 시각"을 벽시계(오늘)로 잡으면 **파이프라인 중단과 백업 중단을 구분 못 한다**
  (파이프라인이 죽어도 gap이 자란다). 기준은 창고의 최신 dt — 파이프라인 신선도는 게이트와
  deadman이 이미 담당하는 관심사 분리를 지킨다.
- Metabase 구독(이메일 발송)은 SMTP 설정이 전제다. 셀프호스트 어플라이언스에 SMTP를 강제할 수
  없으므로 **구독은 문서화된 선택지, 실측 범위는 대시보드까지**.

**판단**: 셋 다 신규 수집 0 — 이미 내린 데이터를 **판정 컬럼까지** 밀어붙이는 일이다. 13단계
(용량 D-day)에서 확립한 원칙 그대로: **lakehouse는 판정 컬럼까지만 계산하고, 발화(알림)는
Metabase(pull) 또는 DBTower(push)의 몫** — 두 번째 알림 시스템을 만들지 않는다. 이 단계가
끝나면 "장기 창고가 있어서 가능한 판정" 3종(용량 D-day·플랜 회귀·백업 공백)이 모두 갖춰지고,
주간 보고는 그 판정들의 요약 서빙이 된다.

### 착수 명세 (Opus) — 16단계

> 전부 이 저장소(lakehouse) 작업, G7만 DBTower. 기존 자산(증분 delete+insert + 워터마크 리터럴
> 패턴, seed+schema contract 패턴, ci_fixture 합성, metabase_bootstrap 패턴, publish 원자성) 최대 재사용.

| # | 조각 | 어디 집 | 구현 명세 | 검증 기준 |
|---|---|---|---|---|
| G1 | 플랜 뒤집힘 이벤트 | lakehouse | `int_plan_flip`(또는 mart 내 CTE): `stg_plan_snapshot`에서 `lag(plan_hash) over (partition by instance_id, query_id order by captured_at)`로 해시가 **실제로 바뀐 행만** 이벤트화 — (instance_id, query_id, flip_at, dt, prev_plan_hash, new_plan_hash). 첫 관측(직전 없음)은 뒤집힘이 아니라 등장이므로 제외 | unit: 동일 해시 반복은 0건, A→B→A는 2건 |
| G2 | mart_plan_regression | lakehouse | 뒤집힘 이벤트별로 `fct_query_daily.avg_latency_ms`를 조인해 **전 N일 vs 후 N일**(기본 N=3, var로 노출) 평균 비교. 컬럼: before_avg_ms, after_avg_ms, latency_ratio, `regressed` 판정(비율 ≥ 임계 and 후기간 호출 수 ≥ 최소치 — 저호출 노이즈 가드), `verdict`(REGRESSED/IMPROVED/NEUTRAL/**PENDING**(후 N일 미도래)/**AMBIGUOUS**(비교창 안에 다른 뒤집힘)). 임계·최소 호출 수는 seed `plan_regression_thresholds.csv`(capacity_thresholds 패턴 — schema contract + 기본행) | unit 3종: 회귀(비율>임계)·개선·PENDING. 합성으로 산식 고정 |
| G3 | 비교창 오염 가드 | lakehouse | (a) 뒤집힘 당일 dt는 전/후가 섞이므로 **양쪽에서 제외**. (b) 같은 쿼리의 인접 뒤집힘 간격 < 2N+1일이면 AMBIGUOUS — 판정을 지어내지 않는다. (c) 후 N일이 아직 안 지났으면 PENDING — 매일 재계산되며 자연 확정 | unit: 당일 제외 산식, 인접 뒤집힘 AMBIGUOUS |
| G4 | mart_backup_rpo | lakehouse | 인스턴스 유니버스는 `fct_query_daily`의 distinct instance_id(전 기종 공통 관측 — **database_instance 신규 추출 없이** 전수 확보)에 `fct_backup_daily`를 left join. 컬럼: last_success_dt(success_runs>0인 최신 dt), last_verified_dt, gap_days(**기준 = 창고 전체 max dt**, 벽시계 아님), `rpo_breach` 판정(gap_days > 임계 or **백업 기록 전무**), `never_backed_up` 플래그. 임계는 seed `backup_rpo_thresholds.csv`(default_max_gap_days + 인스턴스별 override — 로그 백업 주기는 기종별이라 일 단위만 판정, 시간 단위는 라이브(DBTower) 몫으로 명기) | 실데이터: 백업 기록 없는 3개 인스턴스가 never_backed_up=true 행으로 **나타남**(left join 누락 없음) |
| G5 | mart_weekly_ops_report | lakehouse | grain = (week_start, instance_id) 1행. 4계 요약: 용량(min days_to_threshold, worst risk_flag — mart_capacity_forecast), 대기(top 이벤트명·delta_ms — mart_wait_top), 플랜(주간 뒤집힘 수·REGRESSED 수 — G2), 백업(gap_days·rpo_breach — G4). 주 경계는 date_trunc('week') UTC(dow×hour와 같은 결 — 표시만 로컬). materialized=table(주 1회 소비, 증분 불필요 — 전체 재계산이 단순·정확) | 주간 1행/인스턴스, 4계 컬럼이 원천 마트 값과 대조 일치 |
| G6 | 서빙 — 카드·대시보드 | lakehouse | `metabase_bootstrap.py` 패턴으로 "주간 운영 보고" 대시보드: G5 표 1장 + 플랜 회귀 목록(G2 REGRESSED만) + 백업 공백 목록(G4 breach만) 카드. 구독(이메일)은 SMTP 전제라 **문서화만**(RUNBOOK에 설정 절차) — 실측은 대시보드 실화면까지 | 대시보드 실화면 스크린샷, 카드가 판정 컬럼(REGRESSED/breach)으로 필터됨 |
| G7 | MCP 스키마 편입 | DBTower | `lakehouse_query` 도구 설명의 실재 스키마 목록에 신규 마트 3종 추가(15단계 N3 원칙 — 에이전트가 지어내지 않고 실재 컬럼으로 질의). 코드 변경은 도구 설명 문자열 갱신 수준 | MCP로 "지난주 플랜 회귀 있어?" 질의 왕복 |
| G8 | 발행·계보·CI | lakehouse | publish_marts MART_TABLES 10→13(G2·G4·G5), exposures.yml에 주간 보고 대시보드 exposure 추가, ci_fixture에 합성 시나리오(플랜 A→B 뒤집힘+지연 악화, 백업 공백 인스턴스) 편입 — CI가 판정 로직을 회귀 감시 | CI 그린(전 unit·data test), publish 행수 대조 |

**함정(선검증)**:
- (a) **avg_latency_ms의 NULL** — delta_calls=0인 날은 지연이 NULL이다(0 아님, fct_query_daily의
  nullif 설계). 전/후 평균은 NULL 제외 평균으로 계산하고, 관측일 수가 최소치 미만이면 PENDING
  취급 — NULL을 0으로 접으면 회귀를 개선으로 오판한다.
- (b) **뒤집힘 없는 회귀는 이 마트의 몫이 아니다** — 데이터 증가로 같은 플랜이 느려지는 경우는
  mart_query_regression(롤링 랭킹)의 관심사. 겹치지 않게 문서에 경계 명기(둘은 원인 축이 다르다:
  플랜 변경 vs 볼륨 성장).
- (c) **regr 판정의 재현성** — 13단계 CI에서 배운 것(플랫폼별 부동소수): 비율 비교는 반올림한
  값(소수 2자리)으로 고정해 unit test가 러너와 맥에서 같은 답을 내게.
- (d) **백업 공백 판정의 이중 부정** — "백업 테이블에 행이 없음"은 (i) 백업이 안 돎 (ii) 그
  기종의 백업 이력을 DBTower가 아직 수집 안 함, 두 경우가 있다. 지금 실데이터의 3개 부재가
  어느 쪽인지 착수 시점에 원천에서 먼저 확인하고, (ii)면 판정을 UNKNOWN으로 분리 — 안 잰 것을
  잰 척하지 않는다(게이트 SKIP과 같은 결).
- (e) **주간 마트의 빈 주** — 이력이 7일 미만인 초기엔 주간 행이 부분 주다. week_start에
  is_partial_week 플래그를 둬 소비자(대시보드)가 반쪽 주를 온전한 주와 비교하지 않게.

**검증 기준(실측 TODO)**: 합성 뒤집힘 주입 → REGRESSED 판정 재현 + 실데이터 plan_snapshot
판정 실행 + 백업 미기록 인스턴스의 행 존재 + 주간 보고 대시보드 실화면 + CI 그린.

> 실행 기록(2026-07-18 라이브 실측 — VERIFICATION 17절): **G1~G8 구현·검증 완료.** 마트 3종
> (mart_plan_regression·mart_backup_rpo·mart_weekly_ops_report) + seed 2종 + unit test 4종
> (REGRESSED/PENDING/AMBIGUOUS/RPO 산식 고정) + 계약·accepted_values·not_null. 로컬 CI 빌드
> PASS=79(unit 12), pytest 57 그린.
>
> **실데이터 판정(dev 창고)**: (a) **mart_backup_rpo 7행** — 인스턴스 1·2·7은 gap 1일 ok,
> **3·4·8·32는 no_backup_observed**. 함정(d) 확인 결과 이 4건은 "백업 안 돎"이 아니라 **창고가
> 아직 그 백업을 안 받음**((ii) 계열) — 원천(DBTower PG)엔 7개 전부 backup_run이 있으나 aux
> 오프로드(D3 신설)가 fct_backup_daily에 1·2·7의 07-16 하루치만 실었다. 마트가 breach로 단정하지
> 않고 사실만 실어 정확했다. (b) **mart_weekly_ops_report 7행** — 기종별 top 대기를 정직 병기
> (MySQL binlog·PG WalSenderMain·Oracle resmgr:cpu·Mongo wiredTiger·MSSQL RESOURCE_SEMAPHORE),
> is_partial_week=true(주 초). (c) **mart_plan_regression 0행** — 창고의 플랜 이력이 07-16
> **하루뿐**(aux 플랜 오프로드가 최근 신설, 날짜 간 뒤집힘엔 최소 2일 필요)이라 정직한 빈 결과.
> CI 픽스처엔 교차일 뒤집힘을 심어 PENDING 1행 산출(초기 실데이터의 모습)을 확인. 실데이터 판정은
> **시간이 해제**(아래 체크리스트 편입).

---

## 17단계 — 미사용 인덱스 장기 판정: "지워도 되나"는 분기가 답한다 (X1~X4 완료 — 실행 기록 2026-07-18, X5 서빙은 이력 축적 후)

"이 인덱스 지워도 되나"는 7일 관측으론 답할 수 없다. 재시작-누적 카운터의 순간 관측은
"지난주 재기동 이후 0회"와 "분기 내내 0회"를 구분하지 못한다. 정확히 이 저장소만 할 수 있는
판정이다: DBTower가 원료(주기 영속)를 낳고, 여기서 장기 창으로 판정한다.

**전제(DBTower 몫 — 그쪽 ROADMAP "운영 병목 아크 B3")**: `index_usage_snapshot`
(instance_id·table·index·scans·captured_at, 6시간 주기·7일 보존) + `indexUsageStats()`
4기종(PG pg_stat_user_indexes / MSSQL dm_db_index_usage_stats / MySQL
table_io_waits_summary_by_index_usage / Mongo $indexStats), Oracle은 UNSUPPORTED 정직
(MONITORING USAGE 침습·AWR 라이선스). 값 의미는 "재시작 이후 누적".

**지평 경계 — DBTower 라이브 판정과 겹치지 않는다 (2026-07-18 명문화)**: DBTower에는 이미
`UnusedIndexAnalyzer`(FinOps, D6)가 있어 "지금 scanCount=0"인 미사용 후보를 **라이브**로 낸다.
그런데 순간 관측이라 방금 재기동한 서버의 0회도 미사용처럼 보이는 약점이 있고, 그쪽도 "서버
가동 기간을 함께 보라"고 권고에 적는다. `mart_index_verdict`는 그 약점을 메운다 — 90일 창의
일별 실사용 합이라 재기동 노이즈에 안 흔들리고, 관측 부족은 `insufficient_observation`으로
분리한다. **분업은 용량 D-day와 같은 결이다: 단기·즉답은 DBTower 라이브, "분기 내내 정말 안
쓰였나"의 확정은 이 장기 마트.** 원천(indexUsage 통계)은 같고 창(라이브 vs 90일)만 다르다 —
경쟁이 아니라 지평이 갈린 상호보완. 셀프호스터는 "지워도 되나"를 확정할 때 이 마트를,
"지금 당장 훑어보기"는 DBTower 화면을 본다.

| ID | 항목 | 내용 | 검증 기준 |
|---|---|---|---|
| X1 | 레지스트리 편입 | 테이블 스펙 레지스트리에 `index_usage_snapshot` 추가 — 워터마크 captured_at, 게이트 프로필은 wait_event와 동형(행 없음이 정상일 수 있는 축은 SKIP) | 첫 offload 멱등·게이트 통과 |
| X2 | 스테이징·팩트 | 누적 카운터 → 일간 델타는 **2편 first-vs-last + 순리셋 클램프 패턴 그대로 재사용**(재시작 리셋이 음수 델타로 새는 것 방지). `fct_index_daily`(인스턴스·테이블·인덱스·dt·delta_scans) | 리셋 주입 시 클램프 동작 unit test |
| X3 | 판정 마트 | `mart_index_verdict` — 관측 창(예: 90일) 내 delta_scans 합·마지막 사용일·관측 일수. **판정 컬럼은 조언 어휘로**(candidate_unused 등), 삭제 지시 아님 | 창 미달 인덱스가 "관측 부족"으로 분리 |
| X4 | 판정 예외 규칙 | (1) unique/FK 제약 백업 인덱스는 스캔 0이어도 제외 — DBTower 스냅샷에 제약 여부 컬럼이 없으면 X1에서 스펙 확장 요청, (2) 레플리카 전용 사용 오판 한계를 판정문에 명시(프라이머리 통계만 수집), (3) 월말·분기 배치용 장주기 인덱스 — 창 90일의 근거와 한계를 함께 표기 | 예외 3종이 판정문에 실제 표기 |
| X5 | 서빙 | Metabase 카드(후보 목록·마지막 사용일) + `lakehouse_query` 도구로 자연어 질의 가능 확인 | 카드 실물 + MCP 질의 왕복 |

함정: 인덱스명은 재생성 시 동일 이름·다른 실체가 될 수 있다 — (table, index) 키에 최초 관측일을
함께 들고, 사라진 인덱스는 판정 대상에서 제외(존재 여부는 DBTower describeSchema가 진실).

**정직한 한계**: 사용 통계는 프라이머리 기준이라 레플리카 전용 스캔을 못 본다 — 판정을 "삭제"가
아니라 "후보(candidate_unused)"까지만 내는 이유다. 전제(B3 index_usage_snapshot 공급)가 서야
착수한다 — 그 전엔 X1 스펙 협의까지.

> 실행 기록(2026-07-18): **X1~X4 구현·검증 완료.** DBTower B3(V29 index_usage_snapshot)가
> 공급을 열어 착수. X1 레지스트리 편입(extract/tables.py `_INDEX_USAGE_SNAPSHOT` + sources.yml,
> gate completeness=False — Oracle UNSUPPORTED는 행 없음). X2 stg_index_usage_snapshot +
> fct_index_daily(scan_count 누적을 **fct_query_daily와 같은 first-vs-last 델타·GREATEST(0,..)
> 클램프**, 증분 delete+insert·워터마크 리터럴). X3 mart_index_verdict(창 기본 90일, 앵커=최신 dt).
> X4 판정 4갈래(in_use/constraint_backed/insufficient_observation/candidate_unused) — 우선순위는
> 사용중>유니크제외>관측부족>삭제후보, note에 FK 뒷받침·레플리카 전용 사용 미판정 정직 표기.
> **검증**: ci_fixture에 index_usage 픽스처 추가 후 dbt build **PASS=93**(unit test 3종 신규 —
> 델타·리셋 클램프·판정 4갈래, accepted_values로 verdict 값 계약 고정). X5 서빙(Metabase 카드·
> `lakehouse_query` 도구는 15단계에서 이미 존재)은 실이력이 분기만큼 쌓인 뒤.

---

## 18단계 — 설정 드리프트: "이 설정 바뀐 뒤로 느려졌다"의 원인 후보 (구현 완료 — 실행 기록 2026-07-18, VERIFICATION 18절)

**상황 가정**: "이 DB가 2주째 느려졌다"는 신고. 흔한 숨은 원인은 사람이 조용히 바꾼 파라미터
(`work_mem`, `max_connections` 등)다. DBTower는 1시간마다 파라미터를 읽어 변경을 감지하고
알림을 쏘지만(V27 config_snapshot·config_param_change), 그 이력을 **7일류로 지운다**(자체
retention sweep). "3개월 전 언제부터 이렇게 됐나"와 "그 변경 뒤 성능이 나빠졌나"는 장기
이력이 있어야 답하는데, 그게 매일 사라지고 있었다.

**왜 이 저장소인가**: 두 판정 모두 장기 창이 전제다. (1) 설정 변경 타임라인은 분기 단위
되짚기가 필요하고, (2) **상관(변경 → 뒤이은 플랜 회귀)은 장기 설정 이력과 장기 성능 이력이
같은 창고에 있어야만 가능**하다 — DBTower 7일 창은 구조적으로 못 한다. 용량 D-day·플랜 회귀·
백업 공백에 이은 네 번째 "창고라야 가능한 판정"이자, 앞의 판정들에 "왜 그렇게 됐나"의 첫
후보를 붙이는 층이다.

**전제**: producer가 DBTower에 **이미 완성**(V27 테이블 3종 + ConfigDriftDetector 1시간 주기 +
기존 parameters() 5기종 재사용, 신규 operator 0줄). 그래서 이 단계는 **lakehouse 단독 아크** —
DBTower는 GRANT 한 줄(운영)뿐, 코드 변경 0.

### 착수 명세 (Opus) — 18단계

| # | 조각 | 구현 명세 | 검증 기준 |
|---|---|---|---|
| E1 | 레지스트리 편입 | `config_snapshot`·`config_param_change` TableSpec(워터마크 captured_at, 불변 append). **`config_current_param`은 거울(upsert/delete·변이)이라 제외** — 불변 append만 내린다는 계약 유지. 게이트 정합·드리프트만(저빈도·무변경 사이클 다수) | 멱등 오프로드, 게이트 통과 |
| E2 | 스테이징 | `stg_config_snapshot`·`stg_config_param_change`(타입·dt 캐스팅) | 뷰 생성 |
| E3 | fct_config_change_daily | config_snapshot을 **스파인**으로 "수집됐는데 무변경(quiet)"과 "수집 없음(gap)"을 구분(cycles_collected) + config_param_change 상세(change_events·params_changed) | 무변경 인스턴스도 cycles>0·changes=0 행 |
| E4 | mart_config_change | 최근 90일 변경 타임라인 서빙("언제 무엇이 어떻게"). 명시 컬럼(select * 회피 — hive 뷰 위 자기참조 max 서브쿼리가 DuckDB 바인더 내부오류) | 변경 이벤트 시간순 |
| E5 | mart_config_impact | 변경 뒤 N일(기본 7) 내 플랜 뒤집힘/회귀 상관. correlation: followed_by_regression/followed_by_plan_flip/no_flip_observed. **상관이지 인과 아님**(조언 어휘, mart_index_verdict와 같은 결) | unit test로 산식 고정 |
| E6 | 서빙·발행·계보 | publish 3종 편입, exposures(MCP 소비자)·CI 픽스처(변경↔뒤집힘 겹침)·워크플로 env 2종·CONTRACT §1 두 행+GRANT | CI 그린 |

**함정(선검증)**: (a) instance_id·dt가 파일 컬럼과 파티션 경로에 중복 → `select *` + 자기참조
max 서브쿼리 조합에서 DuckDB 내부오류. 명시 컬럼 + anchor CTE(date−정수)로 회피(mart_query_regression
패턴). (b) 초기엔 플랜 이력이 얕아 상관이 대부분 no_flip_observed — 정직한 결과, 이력이 쌓이며
켜진다. (c) "누가"는 대상 DB가 안 줘 원천에 없다 → 마트도 "언제·무엇이"까지(정직 한계).

> 실행 기록(2026-07-18 라이브 실측 — VERIFICATION 18절): **E1~E6 구현·검증 완료.** 로컬 CI
> 빌드 **PASS=106**(unit test 상관 산식 신규·accepted_values), pytest 57 그린. **실데이터(dev
> 창고)**: 원천에 실제 드리프트 존재 — 인스턴스 2·4의 `work_mem`이 4096↔8192로 오르내림.
> 오프로드(config_snapshot 160행·config_param_change 4행) → `mart_config_change` 4행(그 변경
> 그대로), `fct_config_change_daily` 7행(**전 인스턴스 23사이클 수집 + 2·4만 change_events=2**,
> 나머지 0 = 무변경 vs 미수집 구분 실증), `mart_config_impact` 4행(플랜 이력 얕아 no_flip_observed —
> 정직). CI 픽스처엔 변경↔뒤집힘 겹침을 심어 **followed_by_plan_flip** 산출 확인. Metabase "설정
> 드리프트" 대시보드 실화면. 상관의 실데이터 개화는 **시간 해제**(플랜 이력 축적).

---

## 19단계 — 상관의 일반화와 지평 경계 정리 (구현 완료 — 2026-07-18, VERIFICATION 19절)

**상황 가정**: 18단계의 `mart_config_impact`는 설정 변경을 **플랜 뒤집힘 하나**와만 겹쳤다.
그런데 플랜 이력이 얕으면 상관이 안 켜진다. 그리고 원인의 축은 여럿이다 — 파라미터 변경이
꼭 플랜을 뒤집지 않아도 그냥 느려질 수 있고, 바뀌는 대상도 파라미터만이 아니라 스키마(DDL)일
수 있다. 한 소스·한 축에 묶인 상관은 반쪽이다.

**판단(과설계 경계 — 사용자와 확정)**: change_review(스키마 변경, DBTower V28) 전체 오프로드
아크는 **짊어지지 않는다.** 리뷰는 저빈도 감사 데이터라 "버려지는 데이터의 두 번째 삶" 전제가
약하고, 상관 신호도 성기다. 대신 **상관을 일반화**해 소스가 늘면 끼우기만 하면 되게 자리를
열어 둔다. 지금 값이 확실한 두 가지만 한다: (1) 변경 소스 통합, (2) 지연 축 추가.

- **E1 변경 이벤트 통합**: `int_change_events`(view) — 변경을 `(instance_id, change_dt,
  change_source, change_key, change_kind, old/new_value)` 한 형태로 모은다. 지금은 config만,
  **스키마 변경(change_review)은 union all 자리를 코드 주석으로 열어 둠**(편입 시 저빈도
  사후변이라 backup_run식 D+1 계약 필요 — 그때 레지스트리+stg 추가). 주석 안 `ref()`는
  dbt가 의존으로 파싱하니 일부러 안 씀.
- **E2 지연 축**: `mart_config_impact`를 int_change_events 위에서 재계산 + **변경 전후 평균
  지연**(fct_query_daily)을 더한다. correlation을 우선순위로 확장: followed_by_regression >
  followed_by_plan_flip > **followed_by_latency_rise** > no_signal. 플랜이 안 뒤집혀도 "변경
  뒤 느려졌다"를 잡는다. 지연 NULL은 avg 자연 제외(0으로 안 접음).
- **E3 지평 경계 문서**: DBTower `UnusedIndexAnalyzer`(라이브 순간 판정)와 lakehouse
  `mart_index_verdict`(90일 창)가 둘 다 "미사용 인덱스"를 판정 → 어느 걸 믿나. 용량 D-day식
  분업(단기=DBTower / 확정=장기 마트)을 mart 헤더·ROADMAP 17단계·이 절에 명문화. 원천은 같고
  창만 다른 상호보완이지 경쟁 아님.

> 실행 기록(2026-07-18 — VERIFICATION 19절): **E1~E3 완료.** unit test 2종(regression follows·
> latency rise), accepted_values(correlation 4값), 로컬 dbt build **PASS=111**·pytest 57 그린.
> **실데이터**: mart_config_impact가 change_source·before_latency_ms(inst2 3.1ms·inst4 35.04ms
> 실측)까지 채움. after_latency는 변경이 당일(07-18)이라 미래 창이 비어 no_signal(정직 — 후 N일이
> 지나면 켜짐, 시간 해제). MCP 도구 설명에 config 마트 2종 편입(stage 18 누락분). Metabase 카드
> 갱신. change_review는 **미착수(자리만)** — 필요해지면 int_change_events에 소스 하나로 붙는다.

---

## 시간이 해제하는 체크리스트 (2026-07-18 기준 — 코드는 완성, 데이터가 쌓이길 기다림)

> 구현이 남은 게 아니라 **이력이 쌓여야 의미가 생기는** 항목들. 각각 해제 조건과 그
> 시점에 할 일을 못박아 둔다 — "나중에"가 흐지부지되지 않게.

| 항목 | 해제 조건 | 예상 해제 | 그때 할 일 |
|---|---|---|---|
| 롤링 회귀 랭킹(실데이터) | 이력 recent 7 + prior 30일 | ~08-17 | 자동 — 마트가 채워짐(확인만) |
| mart_baseline_longterm 채움 | (dow,hour) 버킷 관측 ≥ 8 = 약 8주 | ~09-08 | 되쓰기 정례화 + deadman 편입 결정 |
| **계절성 오탐 억제 라이브 실증** | 위 + DBTower 4주 창 실버킷 | ~09-08 | 합성 주입 없이 "월요일 피크 무경보" 실측 → DBTower VERIFICATION·블로그 |
| **플랜 회귀 실데이터 판정(16단계)** | plan_snapshot aux 오프로드 이력 ≥ 2일 | ~07-19 | 자동 — mart_plan_regression이 뒤집힘을 잡기 시작(확인만). 초기엔 PENDING 다수 |
| **백업 공백 전 인스턴스 커버(16단계)** | backup_run aux 오프로드가 7기종 전부 누적 | ~07-20 | no_backup_observed가 실제 미백업만 남게 좁혀짐(현재는 창고 미수집분 포함) |
| **C6 용량 대시보드** | capacity learning 해제(관측 14일) | ~08-01 | metabase_bootstrap 패턴으로 카드 구축 + 실화면 → VERIFICATION·7편류 보강 |
| wait 델타 채움 | ~~수 사이클~~ | **해제됨(07-18)** | 21사이클 재추출로 델타>0 52이벤트 — 전부 누적 기종에서만(PG 델타 0 = 스냅샷 기종 정직성의 라이브 증명) |
| MSSQL volume | ~~다음 사이클~~ | **해제됨(07-18)** | 1007GB/774GB 실측 완료 |
| Metabase 60 공식 MCP 재평가 | DuckDB 드라이버의 60 지원 릴리스 | 외부 | N2(카드 도구)와 공식 MCP 비교 재평가(15단계 N4) |
| 12단계 769s 원인 확정 | 대량 backfill이 실제로 필요해질 때 | 조건부 | EXPLAIN 프로파일링으로 가설(창 집계 집중) 확정 |

## dbtower 패밀리 — 무엇이 어디 집인가 (프로젝트 경계, 2026-07-15)

> "새 기능을 dbtower에 붙일까, lakehouse에 붙일까, 아니면 새 레포(`dbtower-????`)를 팔까"의
> 판단 기준을 못박는다. 결론: **레포는 기능이 아니라 "데이터 시야(역할)"로 가른다.** 기능마다
> 레포를 파면 파편화된다 — 강한 포폴은 큰 레포 2개 > 작은 레포 5개.
> (두 레포의 정체성 근거는 "종합 판정 §1"이 원본 — 여기선 그 경계를 *어디에 무엇을 놓나*로 적용한다.)

### 두 레포 (Prometheus→Thanos 구도)

- **dbtower** — 관제탑. 라이브·7일 시야. 대상 DB에 직접 붙어 실시간 읽기. = Prometheus.
- **dbtower-lakehouse** — 장기 분석·창고. 몇 달~년. DBTower가 버리는 데이터의 두 번째 삶. = Thanos.

### 무엇이 어디 (후보 배치)

| 후보 | 집 | 이유 |
|---|---|---|
| 용량 예측(13단계) | **lakehouse** | 증가율 추세 = 장기 이력 필요. 7일로 불가 |
| 월/분기 회귀 추세·장기 베이스라인 | **lakehouse** | 계절성·장기 비교 |
| 기종별 장기 성능·용량 비교(fleet) | **lakehouse** | 수개월 시계열 |
| 고유쿼리 카디널리티 증가 추세 | **lakehouse** | 장기적으로만 보임 |
| MariaDB operator | **dbtower** | 대상 DB 라이브 연결 = 관제 계층 |
| 공유 딥링크 퍼머링크(웹 상태 복원) | **dbtower** | 라이브 웹 UI |
| Mongo COLLSCAN/IXSCAN 뱃지·docsExamined | **dbtower** | 라이브 쿼리 플랜 분석 |
| ~~Tibero~~ | (안 함) | 사용자 결정 |

**경계 규칙**: 대상 DB 접속·최근 데이터가 필요 → **dbtower**. 장기 시계열이 필요 → **lakehouse**.

### 새 레포(`dbtower-????`)를 파는 유일한 명분

역할이 진짜 다를 때만. 예: `dbtower-agent`(대상 서버에 심는 수집 에이전트 — 수천 대에서
pull→push 전환 시). 이건 "수집"이라는 새 역할이라 분리 명분이 선다. **용량 예측은 역할이
"장기 분석"으로 lakehouse와 같아서 새 레포가 아니라 mart로 들어간다.**

### 용량 예측이 두 레포를 잇는 방식

```
dbtower           (tableStats로 크기 주기 스냅샷 — 짧게 보존)          = 낳는다
   │  size_snapshot 을 매일 넘김 (query_snapshot 과 같은 계약)
   ▼
dbtower-lakehouse (장기 아카이브 + 선형회귀 + N일 예측 mart)            = 내다본다
```

---

## 감사 백로그 (8단계 감사에서 정리)

코드 감사가 남긴 항목의 처분을 한곳에 못박는다 — "다음에 한다"와 "안 한다"를
구분하고, 안 하는 것엔 이유를 단다.

### 완료 (9단계에서 소탕 — VERIFICATION 10절)

- [x] **CI 배선 + dbt unit tests** — GitHub Actions 3관문(ruff·pytest·dbt build)이
  커밋마다 강제한다. 델타 로직(first-vs-last·순리셋 클램프·하루 1스냅샷·지문 SUM)은
  dbt unit test 4건으로 고정. 임베디드 DuckDB라 tiny 픽스처로 dbt build를 CI에서 e2e.
- [x] **deadman 알림(heartbeat)** — 성공 heartbeat를 카탈로그 PG에 남기고(DAG 마지막
  태스크), `extract/deadman.py`가 기한 초과 시 경보(Airflow @hourly DAG + 외부 cron).
  30h 침묵·미실행 DAG 경보 발화 실측. '미실행'을 성공의 부재로 잡는다.
- [x] **dbt contracts** — fct·mart에 contract enforced + 컬럼 타입·CHECK 제약. 위반
  주입 시 빌드가 막히는 것(data type mismatch) 실측 → 원복.

(스키마 드리프트 게이트 4축은 백로그였으나 8단계에서 구현 완료 — 유실·타입 변경
FAIL, 초과 컬럼 WARN.)

### 완료 (10단계에서 소탕 — VERIFICATION 11절)

- [x] **365dt 규모 실측 → 증분 전환** — 1년치(2,190파일·54.5M행)를 격리 합성해
  실측: **fct 전체 재빌드 407.62s가 유일 병목**(나머지 초 단위). 이 수치가 증분을
  정당화 → `delete+insert` + 컴파일타임 워터마크 프루닝으로 **407.62s → 4s(~100배)**.
  microbatch는 event_time·unique_key 제약이라 delete+insert 선택(수치·제약 근거).
- [x] **mart 롤링 윈도우 재설계** — "전체 이력 첫날 vs 마지막날" → 최신 dt 기준
  최근 N일 vs 직전 M일(기본 7 vs 30) 롤링 창. 365dt에서 rN=7·pN=30 랭킹 실측.
- [x] **운영 대시보드화** — `pipeline_run_log`(DuckLake) 발행 → Metabase 운영
  대시보드(마지막 성공 dt·오늘 게이트 상태·최근 런). 분석 대시보드와 이원화.

### 안 하기로 한 것 (이유와 함께)

- **mart 증분화**: mart는 전체 이력을 훑는 롤링 윈도우라 dt 독립이 아니지만, 사전집계
  결과라 규모에서도 0.31s다(365dt 실측). 초 단위인 곳을 증분화하면 복잡도만 는다 —
  fct(407s)와 달리 수치가 정당화하지 않는다.
- **microbatch 전략**: dbt-duckdb가 지원은 하나 event_time 필수·unique_key로 파티션
  교체 불가 제약. grain이 (instance,query,dt)인 우리엔 delete+insert가 더 정확·단순.
  event_time 기반 재처리 창이 요구되면 그때.

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
- **범용 쿼리 분석 도구화(아무 Postgres/DB 원천)**: 11단계 판단. "쿼리 성능 장기 저장·분석"은
  이미 성숙한 레드오션(Percona PMM·pgwatch·OpenObserve·pganalyze)이고, OpenObserve는
  Parquet+S3+DataFusion으로 이 스택과 구조가 사실상 같다. solo가 여기 붙으면 열등한
  재구현이 될 뿐이고, 무엇보다 **DBTower와의 결합(관측 원천을 직접 소유)이라는 유일한
  차별점이 죽는다**. 원천은 DBTower `query_snapshot` 계약에 고정한다.
- **Kubernetes/Helm 배포**: 단일 노드 일 배치에 과잉(가드레일 6). 셀프호스트는 docker
  compose standalone까지. 멀티 노드·HA가 실증되면.
- **ML 시계열 예측(ARIMA/Prophet/LSTM)**: 13단계 판단. 용량 예측은 선형회귀+계절 보정으로
  충분하고 상용툴(Redgate·SolarWinds·Oracle Ops Insights)도 선형 외삽이 기본. GB/일은 가장
  예측 가능한 메트릭이라 ML은 이 규모·신호에 과잉(D1 이상감지가 z-score인 것과 같은 결). 선형이
  실측으로 부족함이 확인되면 그때.
- **lakehouse 자체 AI/MCP**: DBTower가 이미 MCP(도구 12종)+자연어 진단+AI 분석을 소유한다.
  lakehouse에 중복하면 데이터 엔지니어링 정체성이 흐려진다 — 두 프로젝트가 "데이터 엔지니어링
  (lakehouse) + 플랫폼/AI(DBTower)"로 폭을 나눠 갖는 게 낫다. 장기 데이터 위 자연어 질의가 정말
  필요해지면, lakehouse가 자체 AI를 갖는 게 아니라 **DBTower MCP가 lakehouse mart를 읽는 도구를
  추가**하는 형태가 자연스럽다.

---

## 실무 실태 조사 (2026-07-09 웹 리서치) — 로드맵의 근거

"다음에 뭘 만들까"를 감이 아니라 실무자들이 실제로 고민하는 문제로 정하기 위해,
서베이·커뮤니티·운영 후기를 조사했다. 요지만 남긴다(출처는 각 항목에).

### Kafka 판정 — 넣지 않는 것이 실무적 정답

조직 단위 채택률과 파이프라인 단위 실사용은 완전히 다른 그림이다.

- 벤더 서베이는 높게 나온다 — Confluent 2025(자기 발주): "86%가 스트리밍 투자
  우선순위". 단 이는 조직 어딘가에서 쓴다는 뜻이지 파이프라인 다수가 스트리밍이란
  뜻이 아니다.
- 실무 바닥은 반대다 — 시니어 패널: "'실시간' 요구의 90%를 물리쳤다",
  "실시간 대시보드가 복잡도를 정당화하는 경우를 아직 못 봤다"
  ([MotherDuck 패널](https://motherduck.com/blog/data-engineers-answer-10-top-reddit-questions/)).
  BigQuery 실사용 분석: 쿼리의 90%가 100MB 미만 처리
  ([Big Data is Dead](https://motherduck.com/blog/big-data-is-dead/)).
  실무자 1,101명 서베이: 20.5%는 오케스트레이션조차 없이 운영
  ([Joe Reis 2026](https://joereis.github.io/practical_data_data_eng_survey/)).
- 채택 조건(양 진영 이견 없음): ①초~분 단위 신선도가 돈이 되고 ②동일 이벤트의
  소비자가 여럿이고 ③분산시스템 운영 여력이 있을 때. 이 파이프라인은 셋 다 아니다
  (스냅샷 원천·일 단위 SLA·소비자 1).
- CDC가 필요해지면 풀 Kafka가 아니라 **Debezium Server(단독)·PeerDB류 경량 CDC**가
  2025-2026 실무 흐름이다 — replication slot 블로트로 원천 디스크가 차는 운영 부담이
  실증돼 있다([slot 관리](https://www.morling.dev/blog/mastering-postgres-replication-slots/)).
  단계: 지금(폴링 배치, 충분) → 학습 목적(CDC의 본질을 Kafka 없이) → 풀 Kafka(소비자
  2+와 분 미만 SLA가 동시에 생길 때만).

### 실무 고통 TOP ↔ 이 프로젝트의 상태

서베이 교차([dbt 2025](https://www.getdbt.com/resources/state-of-analytics-engineering-2025)
· [Monte Carlo](https://www.montecarlodata.com/blog-data-quality-statistics/) ·
[Joe Reis 2026](https://joereis.github.io/practical_data_data_eng_survey/)) 기준 순위:

| 실무 고통 (근거 수치) | 이 프로젝트 |
|---|---|
| 1. 조용한 실패 — 스테이크홀더가 먼저 발견 74%, 해결 평균 15시간 | 품질 게이트 4축 fail-closed + webhook (4·6·8단계) + **deadman heartbeat (9단계)** — '미실행'까지 대응 완료 |
| 2. 스키마 변경이 다운스트림 파괴 | 드리프트 게이트 (8단계) + **dbt contracts (9단계)** — 발행 전 빌드 차단으로 마감 완료 |
| 3. 비용 통제 | 로컬 스택의 대응물은 스캔량·저장량 — CHECKPOINT가 파일·바이트 절감 실측 (6단계) |
| 4. 백필/멱등 — "안전하게 재실행되는 파이프라인"이 프로덕션 등급의 기준 | 파티션 덮어쓰기 멱등 + backfill 실증 + 자기파괴 가드 (1·6·8단계) — 대응 완료 |
| 5. 작은 파일/파티션 폭증 — 최대 4배 저하, 128MB~1GB/파일이 합의 타깃 | DuckLake CHECKPOINT 컴팩션 (6단계). 규모 실측(백로그 3)이 다음 |

즉 이 로드맵의 다음 항목들(CI·deadman·규모 실측·롤링 윈도우·contracts)은 실무 고통
1~5위와 그대로 겹친다 — 스트리밍 도입보다 이쪽이 실무가 향하는 방향이다.

### 스택 위치 판정

Airflow(오케스트레이터 점유 1위, PyPI 다운로드 2위의 10배) + dbt + Parquet/객체
스토리지 + DuckDB(프로덕션 침투 실증 다수) + DuckLake(v1.0, 단일 엔진·low-TB·
잦은 커밋 패턴에 적합 판정) + Metabase 조합은 2026년 기준 소규모 팀의 정석 계보다.
멀티엔진 연합이 필요해지는 시점의 표준은 Iceberg이고, 전환은 어댑터 문제로 남긴다
([DuckLake vs Iceberg 운영 후기](https://www.definite.app/blog/duck-lake-vs-iceberg),
[오케스트레이션 실태](https://www.pracdata.io/p/state-of-workflow-orchestration-ecosystem-2025)).

---

## 블로그 계획

새 시리즈(카테고리 분리, DBTower와 별개). 각 편 개선 아크. DBTower 0편에서 "관측 데이터의
다음 여정"으로 상호 링크.

- 발행됨: 0~6편(단계별) · **7편**(11단계 셀프호스트) · **8편**(12단계 N축) · **9편**(14단계
  레지스트리·되쓰기) · DBTower 측 **19편**(15단계 자연어 서빙 — 그쪽 시리즈).
- 예정: **10편** — 16단계(판정 3종 완결: 용량 D-day·플랜 회귀·백업 공백 + 주간 보고).
  구현 → 라이브 실측 → 실화면 스크린샷 → 작성 리듬 유지.

## Sources (전제·함정 검증)
- [AWS PI 7일 무료 보존](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PerfInsights.Overview.cost.html)
- [Airflow data intervals 심화](https://towardsdatascience.com/airflow-data-intervals-a-deep-dive-15d0ccfb0661/)
- [Airflow backfill 함정](https://risingwave.com/blog/avoiding-airflow-backfill-pitfalls-expert-advice/)
- [Airflow 멱등 dbt 태스크](https://tomasfarias.dev/articles/writing-idempotent-dbt-tasks-for-airflow/)
- [DuckLake 공식](https://ducklake.select/) · [dbt Model governance](https://docs.getdbt.com/docs/mesh/govern/about-model-governance)
- [추출 best practice(CDC/replica)](https://www.automq.com/blog/fivetran-vs-airbyte-elt-tools-comprehensive-comparison)
- 11단계(셀프호스트) — [Airflow security](https://airflow.apache.org/docs/apache-airflow/stable/security/index.html) · [Airflow Fernet key](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html) · [미설정 Airflow 노출 사고](https://www.darkreading.com/vulnerabilities-threats/misconfigured-apache-airflow-platforms-threaten-organizations)
- 11단계 — [production docker compose best practice](https://nickjanetakis.com/blog/best-practices-around-production-ready-web-apps-with-docker-compose) · [compose in production(이미지 태그)](https://distr.sh/blog/running-docker-in-production/) · [self-host compose 예시](https://github.com/Haxxnet/Compose-Examples)
- 11단계 — [Metabase H2→Postgres 이관](https://www.metabase.com/docs/latest/installation-and-operation/migrating-from-h2) · [리버스 프록시로 감싸기](https://medium.com/@impiyush/stop-exposing-your-self-hosted-services-do-this-instead-6e327a0c69a0)
- 11단계(범용화 안 함 근거) — [pganalyze 대안·PG 모니터링](https://uptrace.dev/tools/postgresql-monitoring-tools) · [OpenObserve(Parquet+S3+DataFusion)](https://openobserve.ai/)
- 12단계(N축 규모) — [단일노드 데이터 엔지니어링(DuckDB 스케일링)](https://iceberglakehouse.com/posts/2026-05-23-single-node-data-engineering-duckdb-datafusion-polars-lakesail/) · [Big Data is Dead](https://motherduck.com/blog/big-data-is-dead/)
- 12단계(소파일) — [Iceberg 소파일 가이드](https://lakeops.dev/blog/iceberg-small-files-guide) · [DuckDB+Iceberg 소파일 트랩·컴팩션](https://medium.com/@hadiyolworld007/duckdb-iceberg-without-pain-partitioning-compaction-and-the-small-files-trap-in-local-first-3686a6a86e12)
- 12단계(오케스트레이터 전환) — [Airflow executor 비교(Astronomer)](https://www.astronomer.io/docs/learn/airflow-executors-explained)
- 13단계(용량 예측) — [Database capacity planning 방법론(GB/일 증가율)](https://www.jusdb.com/blog/database-capacity-planning-metrics-growth) · [SolarWinds 디스크 포캐스트(선형 외삽)](https://www.solarwinds.com/sql-sentry/use-cases/database-storage-forecasting-tool) · [Oracle Ops Insights capacity planning](https://docs.oracle.com/en-us/iaas/operations-insights/doc/database-capacity-planning.html)
- 13단계(임계 원천·오토스케일) — [MSSQL sys.dm_os_volume_stats(볼륨 total/available)](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-volume-stats-transact-sql?view=azuresqldb-current) · [RDS storage autoscaling(max threshold·6h 쿨다운)](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PIOPS.Autoscaling.html) · [K8s PVC 확장(allowVolumeExpansion — 자동 아님)](https://kubernetes.io/docs/concepts/storage/persistent-volumes/)
