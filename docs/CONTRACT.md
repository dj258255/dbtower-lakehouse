# 데이터 계약 (Data Contract) — raw 레이어

> 파이프라인 버그의 대부분은 "계약 불명확"에서 온다: dt 경계가 UTC냐 KST냐,
> 파티션 키가 뭐냐, 스키마가 진화하면 옛 파일은 어떻게 읽냐.
> 이 문서는 Extract & Load(1단계)가 지키는 계약을 못박는다. 코드는 이 문서를 따른다.

## 1. 원천 (Source)

- **위치**: DBTower 메타 PostgreSQL (`dbtower-postgres`, 호스트 포트 15432, DB `dbtower`).
  이건 **관측 전용 메타 DB**다. 운영 대상 DB(mysql/oracle/…)는 절대 건드리지 않는다.
- **테이블 (두 개 — 둘 다 read-only 권한 필요)**:
  | 테이블 | 역할 | 읽는 것 |
  |---|---|---|
  | `query_snapshot` | 팩트(쿼리 성능 스냅샷 원본) | 전체 컬럼(아래 스키마) |
  | `database_instance` | 레지스트리(어느 인스턴스를 훑을지 = 루프 드라이버) | `id` (`offload.py::_list_instance_ids`) + 품질 게이트 completeness의 기대 인스턴스 집합 |

  > **왜 두 개인가**: `query_snapshot`을 `captured_at`만으로 훑으면 인덱스
  > `idx_snapshot_instance_time(instance_id, captured_at)`의 선두 컬럼을 못 탄다(가드레일 1 —
  > 원천 무부하). 그래서 `database_instance`에서 인스턴스 목록을 먼저 얻어 **인스턴스별
  > 등치 질의**를 돈다. `database_instance`에 SELECT 권한이 없거나 비어 있으면 인스턴스 0개로
  > 판정돼 **아무것도 안 뽑고 조용히 빈 결과**가 난다(셀프호스트 install 시 최다 함정).
- **접근**: 읽기 전용 세션(`SET TRANSACTION READ ONLY`) + instance별 시간창 질의 + 서버커서 배치.

### 1-1. 다중 원천 테이블 (Phase 14 — 레지스트리 `extract/tables.py`가 단일 진실)

| 테이블 | 워터마크 | 불변성 | 게이트 프로필 | 상태 |
|---|---|---|---|---|
| `query_snapshot` | `captured_at` | 불변 | 4축 전부 | 주 파이프라인 |
| `backup_run` | `started_at` | **사후 변이**(verify/remote가 나중에 UPDATE) → **D+1 스냅샷 계약**: 어제 dt를 오늘 뽑아도 이후 갱신될 수 있다. 워터마크는 불변인 `started_at` | 정합·드리프트만(저빈도 — 전 인스턴스 백업이 정상 아님 → completeness 오탐, 이벤트성 → freshness 오탐) | 추출 중 |
| `plan_snapshot` | `captured_at` | 행은 불변, **보존이 카운트 기반**(쿼리당 최신 20개 스윕) → D2(DBTower: 시간 기반 보존 병행) 전까지 당일 추출분 불완전 가능 — 정직 표기 | 정합·드리프트만 | 추출 중 |
| `wait_event_snapshot` | `captured_at` | 불변(5분 주기 append, 7일 보존) | 정합·신선도·드리프트(completeness만 끔 — 미지원/무대기 기종은 사이클에 행이 없는 게 정상) | 추출 중(D1 — DBTower V25, 2026-07-18 편입). **기종별 의미 차이 계승**: wait_count·total_ms가 MySQL/MSSQL/Oracle=누적, PG=현재 스냅샷, Mongo=대기 큐 — 마트가 기종별로 해석해야 한다 |

프로필이 끈 축은 게이트 보고서에 **SKIP**으로 남는다(안 잰 것을 잰 척하지 않는다).
GRANT 목록: `GRANT SELECT ON query_snapshot, database_instance, backup_run, plan_snapshot, wait_event_snapshot TO lakehouse_reader;`

### 1-2. 되쓰기(writeback) — 분석계→원천 방향의 유일한 쓰기 (Phase 14 D7)

- **대상**: 원천 쪽 별도 테이블 `baseline_longterm`(DDL 소유권은 DBTower Flyway — D8).
  화물은 DuckLake에 발행된 `mart_baseline_longterm`(장기 dow×hour 베이스라인).
- **역할 분리**: `lakehouse_writer`(해당 테이블만 SELECT/INSERT/DELETE) — **원천 읽기
  계정(`lakehouse_reader`)·SourceConfig와 무관**. 원천 관측 테이블 readonly 봉인은 불변
  (권한 격리 실측: writer로 `query_snapshot` SELECT 시 permission denied — VERIFICATION).
- **원자성**: DELETE+INSERT 단일 트랜잭션 + 행수 대조(다르면 롤백) — DBTower 폴러가
  도중에 읽어도 MVCC로 이전 버전을 본다(빈 테이블 순간 없음).
- **선택 기능**: `WRITEBACK_PG_*` 미설정이면 no-op 스킵(명시 로그). 베이스라인이 비면
  (이력 부족·top-K 잘림) DBTower는 현행 7일 창으로 폴백 — 오류가 아니라 계약.

### 원천 스키마

| 컬럼 | 타입(PG) | 의미 |
|---|---|---|
| `id` | bigint | PK(감사·중복 추적). |
| `instance_id` | bigint | `database_instance` FK. **파티션 키**. |
| `captured_at` | timestamp(6) (UTC) | 수집 시각. 같은 값이 한 배치. **워터마크·dt 경계 기준**. |
| `query_id` | varchar(64) | 쿼리 지문 해시. |
| `query_text` | varchar(4000) | 정규화된 쿼리문(nullable). |
| `calls` | bigint | **누적** 호출수(서버 기동 이후 단조 증가 카운터). |
| `total_time_ms` | double precision | **누적** 총 실행시간 ms. |
| `rows_examined` | bigint | **누적** 검사 행수. |

인덱스: `idx_snapshot_instance_time (instance_id, captured_at)`.

### calls/total_time_ms의 의미 — 누적 vs 구간 (확인 완료)

**결론: 누적(cumulative) 카운터다. 구간값이 아니다.** 두 경로로 확인했다.

1. **코드**: `QuerySnapshot` 엔티티 javadoc — "쿼리별 **누적 통계** 한 줄 … 시점 비교는
   구간 양 끝 배치의 **카운터 차분**으로 계산한다." `ComparisonService`가
   `end.getCalls() - start.getCalls()` 로 델타를 구하고 `Math.max(0, …)` 로
   카운터 리셋(대상 재기동)을 클램프한다.
2. **실측**: 한 쿼리를 시간순으로 뽑으면 `calls`가 61 → 204 → 348 → 700 → 1348 → 1732 로
   단조 증가하다가, 유휴 구간에는 1732로 평탄하게 유지된다(감소 없음). 누적 카운터의 전형.

1단계(EL)은 **원본을 그대로 내리므로** 이 판단 없이도 정확하다.
누적→일간 델타 변환은 2단계(dbt)의 몫이다. 여기선 사실만 기록한다.

## 2. 적재 (Sink)

- **저장소**: MinIO(S3 호환, `dbtower-minio`, 호스트 포트 19000). 버킷 `lakehouse`.
- **포맷**: Parquet + **zstd** 압축. 스키마는 **명시 선언**한다(타입 추론 변화 차단).

### parquet 스키마 (고정 선언, `extract/offload.py::SNAPSHOT_SCHEMA`)

| 필드 | Arrow 타입 |
|---|---|
| `id` | int64 |
| `instance_id` | int64 |
| `captured_at` | timestamp(us) |
| `query_id` | string |
| `query_text` | string (nullable) |
| `calls` | int64 |
| `total_time_ms` | float64 |
| `rows_examined` | int64 |

### 파티셔닝

```
s3://lakehouse/raw/query_snapshot/dt=YYYY-MM-DD/instance_id=N/part-000.parquet
```

- `dt` = 논리 날짜(= data_interval_start의 날짜, **UTC**). captured_at의 날짜와 일치.
- `instance_id` = 인스턴스별 물리 분리. 인덱스 선두 컬럼을 타는 질의 단위이기도 하다.
- Hive 스타일 파티셔닝 → DuckDB `read_parquet(..., hive_partitioning=1)` 로 `dt`/`instance_id`를
  컬럼으로 직독.

## 3. 워터마크 · 시간 경계

- **워터마크 = `data_interval`**. `@daily` DAG는 실행 시 `[data_interval_start, data_interval_end)`
  = 하루 구간을 받는다. 태스크는 `data_interval_start.date()`(= "어제")를 논리 날짜로 쓴다.
- **경계는 UTC**. DBTower가 `captured_at`을 UTC로 저장하므로(hibernate `time_zone=UTC`),
  파티션 경계도 UTC 자정으로 잡아 KST/DST로 인한 경계 흔들림을 없앤다.
- 질의 조건: `captured_at >= dt 00:00:00 AND captured_at < (dt+1) 00:00:00` (반열림 구간, 겹침·누락 0).

## 4. 멱등성 (Idempotency)

- 재적재 단위 = **파티션(dt, instance_id)**. 적재 전 해당 프리픽스를 통째로 삭제하고
  단일 `part-000.parquet`를 새로 쓴다(**whole-partition overwrite**).
- 같은 dt를 몇 번 돌려도 오브젝트는 인스턴스당 1개, 행수는 원천과 항상 동일.
  (닫힌 구간 2회 실행 → 79,894행 불변, 오브젝트 6개 유지로 실측. `docs/VERIFICATION.md` 2절.)

## 5. 스키마 진화

- raw(parquet 덮어쓰기) 레이어: 컬럼 추가는 append-only(뒤에 붙임)만 허용,
  기존 컬럼 타입 변경 금지. 원천 스키마를 그대로 따라간다.
- 테이블 포맷(5단계, DuckLake): 스키마가 카탈로그(PG)에 박히고 변경이 버전으로
  쌓인다. ADD COLUMN 등 진화가 스냅샷으로 기록돼 옛 버전 질의가 계속 가능하다.

## 6. 품질 게이트 (3단계 — 다운스트림 전 검문)

raw가 반쪽만 적재된 채 dbt 마트가 만들어지면 랭킹이 조용히 오답을 낸다. 다운스트림(dbt)에
넘어가기 전 `extract/quality.py`가 dt 파티션을 세 축으로 검문하고, FAIL이면 변환을 차단한다(fail-closed).

| 검문 | 규칙 | 실패 |
|---|---|---|
| reconciliation | 원천 PG 행수 == parquet 행수(인스턴스별) | FAIL(차단) |
| completeness | 레지스트리 기대 인스턴스가 dt 파티션에 전부 존재 | FAIL(차단) |
| freshness | dt 최신 captured_at이 다음날 00:00에 근접(기본 WARN 3h / FAIL 12h) | WARN/FAIL |

- 오케스트레이션: `extract/run_pipeline.py`(게이트→dbt run) 및 Airflow DAG
  `offload → quality_gate → transform → publish`. 게이트 FAIL 시 transform 이후는 실행되지 않는다.
- 실측: `docs/VERIFICATION.md` 5절(정상 통과 + 장애주입 FAIL + Airflow 차단).

## 7. 운영 계약 (6단계 — 실패는 통보되고, 과거는 절차로만 재적재한다)

- **실패 통보**: 태스크가 최종 실패(재시도 소진)하면 `ALERT_WEBHOOK_URL`로 JSON POST
  (dag/task/실행일/로그 URL/에러 요약). 알림 실패는 파이프라인 상태에 영향 없음(best-effort).
- **retry**: 일시 장애는 retries=3 + 지수 백오프로 흡수. 단 품질 게이트는 retries=0 —
  품질 FAIL은 결정적이므로 재시도하지 않는다.
- **backfill**: catchup=False 고정. 과거 dt 재적재는 `docs/RUNBOOK.md` 절차로만.
  멱등(파티션 통째 덮어쓰기)이 계약이므로 backfill은 몇 번이든 안전하다.
- **DuckLake 유지보수**: 스냅샷·파일 보존 `DUCKLAKE_RETENTION`(기본 7일, 원천 보존과 대칭).
  주간 CHECKPOINT가 그보다 오래된 버전을 만료·정리한다 — 그 이전으로의 타임트래블은
  보장하지 않는다. 유지보수는 현재 상태(행수)를 절대 바꾸지 않는다(불변식 검사).
- **서빙(7단계)**: 대시보드(Metabase)는 dbt의 DuckDB 파일을 직접 열지 않는다 —
  `publish` 태스크가 DuckLake로 발행한 마트만 read-only로 읽는다(파일은 프로세스 간
  단일 쓰기라 BI가 물면 transform과 충돌 — VERIFICATION 8-2절 실측). 발행은 통째
  교체(DROP+CREATE 한 커밋)이고 발행 후 행수를 원본과 대조한다(다르면 실패).
- **생존 신호(9단계)**: `snapshot_offload` 성공 시 마지막 태스크 `heartbeat`가
  `ducklake_catalog.pipeline_heartbeat`에 성공 시각을 남긴다(메타 DB 비오염). 기한
  (기본 26h) 내 갱신이 끊기면 `deadman_watch`(@hourly)와 외부 cron이 역방향으로
  경보한다 — 태스크 미실행(스케줄러 death·pause·원천 침묵)까지 잡는다.
- **마트 계약(9단계)**: `fct_query_daily`·`mart_query_regression`은
  `contract: enforced: true`로 컬럼 이름·타입·제약(not_null·delta>=0 CHECK)을 선언한다.
  모델 산출이 계약과 어긋나면 빌드가 막힌다(dbt-duckdb DB 레벨 enforce). 계약은
  `models/marts/schema.yml`이 단일 진실. 커밋마다 CI(`.github/workflows/ci.yml`)가
  픽스처로 이 계약을 강제 검증한다.

## 8. 비계약(아직 보장 안 하는 것)

- 지연 도착(late-arriving) 처리: raw는 "그 시점의 원천 스냅샷"만 보장.
- 통계적 이상 자동 감지: 품질 게이트는 규칙 기반까지(실패 통보는 7절로 계약에 들어옴).
- 알림 심각도 라우팅·중복 억제: 채널 하나에 best-effort POST까지.
- ACID·타임트래블: 5단계(DuckLake)에서 제공. raw 레이어 자체는 여전히
  "lake"(파티션 덮어쓰기)이고, 그 위에 테이블 포맷을 얹은 것이 house다.
- 크기 스냅샷(`size_snapshot` — 테이블/디스크 크기 시계열): **아직 계약에 없다.**
  용량 예측(ROADMAP 13단계)이 요구하는 세 번째 원천으로 계획만 있고, DBTower 쪽
  공급(tableStats 주기 저장)과 함께 착수 시 이 문서에 정식 편입한다.
