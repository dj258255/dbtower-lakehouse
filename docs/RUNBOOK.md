# RUNBOOK — 장애 대응·backfill·유지보수 절차

> 파이프라인은 언젠가 실패한다. 문서 없는 파이프라인은 실패하는 날 만든 사람의 기억력
> 테스트가 된다. 이 문서는 "새벽에 알림을 받은 사람"이 처음부터 끝까지 따라갈 수 있는
> 절차를 못박는다. 모든 명령은 실측으로 검증했다(docs/VERIFICATION.md 7절).

## 0. 구성 요약

| 구성 | 위치 |
|---|---|
| DAG | `snapshot_offload`(@daily, offload → quality_gate → transform → publish → heartbeat) · `ducklake_maintenance`(@weekly) · `deadman_watch`(@hourly, 6절) |
| 실행 환경 | `lakehouse-airflow-scheduler` 컨테이너 (커스텀 이미지 — Dockerfile, dbt는 /opt/dbt-venv) |
| 원천 | DBTower 메타 PG `dbtower-postgres` (읽기 전용) |
| 싱크 | MinIO `dbtower-minio`, 버킷 `lakehouse` |
| 알림 | `ALERT_WEBHOOK_URL`로 JSON POST (`extract/alerts.py`, 미설정 시 no-op) |

## 1. 실패 대응 절차 (알림 → 로그 → 재적재)

태스크가 최종 실패하면(재시도 소진) webhook으로 아래 형태의 JSON이 온다.

```json
{"event": "airflow_task_failed", "dag_id": "snapshot_offload", "task_id": "quality_gate",
 "logical_date": "...", "try_number": 1, "log_url": "http://.../log?...", "error": "품질 게이트 FAIL — ..."}
```

### 1-1. 어떤 태스크가 죽었나로 분기한다

| 실패 태스크 | 뜻 | 대응 |
|---|---|---|
| `offload` | 원천 PG/MinIO 접속·추출 실패. 재시도 3회(지수 백오프)를 이미 소진한 상태 | 2단계로 — 인프라부터 본다 |
| `quality_gate` | **데이터가 틀렸다**(정합·완결성·신선도). 재시도 없음 — 결정적 실패 | 3단계로 — 원인 dt를 본다 |
| `transform` | dbt run 또는 test 실패(모델 SQL·테스트 위반) | 로그의 dbt 출력으로 어느 모델/테스트인지 특정 |
| `checkpoint`(ducklake_maintenance) | 카탈로그 PG·S3 순단 또는 유지보수 불변식 위반 | 행수 변동 에러면 **즉시 조사**(데이터 손실 신호), 순단이면 재트리거 |

### 1-2. 인프라 확인 (offload 실패 시)

```bash
docker ps --format '{{.Names}}\t{{.Status}}' | grep -E 'dbtower-postgres|dbtower-minio'
docker exec dbtower-postgres pg_isready -U postgres          # 원천 PG
curl -s --max-time 5 http://localhost:19000/minio/health/live -o /dev/null -w '%{http_code}\n'  # MinIO
```

죽어 있으면 `cd ~/Desktop/dbtower && docker compose up -d` 후, Airflow UI에서 실패 태스크
**Clear** (또는 아래 backfill). 접속은 전부 connect_timeout=5라 무한 대기는 없다 —
실패는 5초 안에 드러나고 재시도가 흡수한다.

### 1-3. 게이트 FAIL 확인 (quality_gate 실패 시)

로그 URL에서 어느 검문이 FAIL인지 본다. 호스트에서 같은 검문을 재현할 수 있다.

```bash
.venv/bin/python -m extract.quality 2026-07-06        # 문제 dt만 지정
```

- **reconciliation/completeness FAIL** → 해당 dt 파티션이 유실·반쪽. 재적재:
  `.venv/bin/python -m extract.offload 2026-07-06` (멱등 — 파티션 통째 덮어쓰기라 몇 번 돌려도 안전)
  후 게이트 재실행으로 PASS 확인, Airflow에서 실패 런 Clear.
- **freshness FAIL** → 그 날 수집이 중간에 끊겼다. 원천(DBTower) 수집기 상태부터 확인.
  데이터가 정말 없는 거면 재적재로 해결되지 않는다 — 결측 dt로 기록하고 넘어간다(정직 표기).

## 2. backfill 레시피

`catchup=False`는 유지한다 — DAG 정지·재배포 후 Airflow가 밀린 날짜를 **무의도로 대량
자동 실행**하는 걸 막기 위해서다(0단계 결정). 과거 날짜는 아래처럼 **명시적으로만** 돌린다.

### 2-1. 날짜 산수부터 — 논리 실행일과 처리 dt는 하루 어긋난다

`-s/-e`는 **논리 실행일 구간(양끝 포함)**이고, @daily에서 각 런은 **논리일의 전날
dt**를 처리한다(실측: `-s 2026-07-06 -e 2026-07-07` → 런 2개 생성, 각각
dt=2026-07-05·dt=2026-07-06 처리 — VERIFICATION 7-4절).

> **dt=D 하나만 재적재하려면 `-s D+1 -e D+1`.**

dry-run으로 선검증할 수 있다.

```bash
docker exec lakehouse-airflow-scheduler \
  airflow dags backfill snapshot_offload -s 2026-07-07 -e 2026-07-07 --dry-run
```

첫 줄 `Dry run of DAG snapshot_offload on <논리일>`로 어떤 런이 생기는지 확인한다.
알려진 제약: TaskFlow에서 XCom을 입력으로 받는 태스크(quality_gate 등)는 업스트림이
실제로 안 돌면 렌더가 안 돼 dry-run이 뒤에서 에러로 끝난다(종료코드 1) — 날짜 구간
확인 용도로만 쓰고, 에러 자체는 무시해도 된다.

### 2-2. 실제 backfill

```bash
docker exec lakehouse-airflow-scheduler \
  airflow dags backfill snapshot_offload -s 2026-07-07 -e 2026-07-07 --reset-dagruns -y
```

- `--reset-dagruns`: 같은 구간에 기존 런(성공 포함)이 있으면 지우고 다시 돈다 —
  재적재가 목적이므로 필요.
- 안전 근거: **멱등**(파티션 통째 덮어쓰기 — 같은 dt를 몇 번 돌려도 행수 불변,
  VERIFICATION 2·7절 실측) + **동시성 상한**(max_active_runs=1,
  offload max_active_tis_per_dag=1, 코어 PARALLELISM=4)이라 넓은 구간을 걸어도
  런이 순차로 흘러 원천·스케줄러를 짓누르지 않는다.
- 여러 날짜 구간도 같은 명령에 `-s/-e`만 넓히면 된다. 단 원천 스냅샷 보존이 7일이므로
  **7일보다 오래된 dt는 원천에 이미 없다** — backfill 가능 창은 최근 7일이다.
- **보존 창 밖 dt를 실수로 backfill/Clear 하면** offload가
  `ArchiveSelfDestructError`로 시끄럽게 실패한다(8단계 가드). 원천이 0행인데
  기존 parquet 파티션이 존재하면 그 파티션이 **유일본**일 수 있어 삭제를 거부하는
  것 — 파티션은 그대로 보존되고 알림(webhook)이 온다. 이 에러가 오면 재시도하지
  말고 dt를 다시 확인하라. 정말 지워야 하는 파티션이면 사람이 MinIO에서 명시적으로
  지운 뒤 재실행한다.

### 2-3. 검증

```bash
.venv/bin/python -m extract.quality 2026-07-06     # 게이트 4검문 PASS 확인
```

### 2-4. 증분 fct와 과거 dt 정정 (10단계)

`fct_query_daily`는 증분(delete+insert)이라, 매일 `dbt run`은 최신 dt만 다시 계산한다
(407s→4s). 그런데 파티션 프루닝 워터마크가 `dt >= max(dt)`라, **이미 지나간 과거
dt(< max)를 정정**하면 일반 `dbt run`은 그 dt를 건드리지 않는다. 과거 dt를 offload로
다시 내린 뒤에는 fct를 명시적으로 다시 지어야 한다:

```bash
# 과거 dt(예: max보다 이전) 정정 후 — 그 모델만 전체 재빌드
cd dbt/dbtower_lakehouse
dbt run --select fct_query_daily+ --full-refresh --profiles-dir . --project-dir .
```

`+`로 다운스트림(mart)까지 함께 재빌드한다. 최신 dt 재실행(같은 날 재시도)은
워터마크 `>=`가 잡으므로 `--full-refresh` 없이 멱등하게 갱신된다.

## 3. DuckLake 유지보수

- **왜**: DuckLake는 커밋마다 스냅샷을 쌓고 덮어쓰인 파일을 타임트래블용으로 남긴다.
  스스로는 아무것도 지우지 않으므로 방치하면 카탈로그(PG)·스토리지(S3)가 단조 증가한다.
- **주기**: `ducklake_maintenance` DAG가 @weekly로 CHECKPOINT 번들(만료+플러시+컴팩션,
  공식 권장 — 만료·컴팩션을 손으로 따로 부르면 순서 이슈가 있다)과 삭제 예약 파일
  정리를 돌린다. 보존 기간은 `DUCKLAKE_RETENTION`(기본 7 days — 원천 보존 7일과 대칭).
- **수동 실행**(호스트):

```bash
.venv/bin/python -m extract.ducklake_maintenance                     # 보존 7일
.venv/bin/python -m extract.ducklake_maintenance --retention '30 days'
```

- **불변식**: 유지보수는 현재 상태를 절대 바꾸지 않는다. 모듈이 전/후 행수를
  **테이블별로** 대조해 다르면 예외를 던진다(그 예외도 webhook으로 온다). 이 에러는
  항상 즉시 조사 대상.
- **대상**: 카탈로그에 지금 존재하는 테이블 전체(마트 포함)를 잰다(8단계 — 특정
  테이블 하드 참조 제거). 테이블이 하나도 없는 새 환경에서도 죽지 않고 스냅샷·고아
  파일 정리만 하고 지나간다.
- **데모 주의**: `python -m extract.ducklake_load`(ACID·타임트래블 데모)는 기존
  query_snapshot을 DROP 후 재생성한다. 기존 테이블이 있으면 확인 없이는 중단된다 —
  재생성 의도가 확실할 때만 `--force` 또는 `DUCKLAKE_DEMO_FORCE=1`.
- **트레이드오프**: 보존 기간보다 오래된 버전으로의 타임트래블은 포기한다. 대신 용량이
  유계가 된다. raw parquet 원본은 별도 경로에 그대로 있으므로 데이터 자체는 잃지 않는다.

## 4. 알림 배선 점검

수신기(Slack/Discord/사내 웹훅)를 바꿨거나 알림이 안 오는 것 같을 때, 파이프라인을
돌리지 않고 배선만 점검한다.

```bash
docker exec -w /opt/airflow lakehouse-airflow-scheduler python -m extract.alerts
# INFO 알림 전송 완료 → ... (HTTP 200) 이 떠야 정상
```

- `ALERT_WEBHOOK_URL`은 `.env`로 주입되며 **컨테이너 관점 주소**여야 한다
  (호스트에서 듣는 수신기면 `http://host.docker.internal:PORT/...`).
- 알림 실패는 파이프라인을 죽이지 않는다(try/except로 삼킴). 즉 "알림이 안 왔다"가
  "파이프라인이 안 돌았다"를 뜻하지 않는다 — UI/`airflow dags list-runs`가 최종 진실이다.

## 5. 대시보드 (Metabase) 운영·재현

### 5-1. 구성

| 구성 | 값 |
|---|---|
| 컨테이너 | `lakehouse-metabase` (compose `metabase`, 커스텀 이미지 `metabase/Dockerfile`) |
| 포트 | http://localhost:13001 (13000은 dbtower-grafana) |
| 관리자 | `.env`의 `METABASE_ADMIN_EMAIL` / `METABASE_ADMIN_PASSWORD` |
| 연결 대상 | **DuckLake**(카탈로그 PG `ducklake_catalog` + MinIO), read-only |
| 데이터 공급 | `snapshot_offload`의 `publish` 태스크가 dbt 마트를 DuckLake로 발행 |
| 앱 DB | H2(named volume `metabase-data`) — 지우면 대시보드도 사라진다 |

Metabase는 dbt의 DuckDB **파일을 직접 물지 않는다**. 파일은 프로세스 간 단일 쓰기라
BI가 물면 transform과 충돌한다(같은 호스트에선 잠금 충돌, 컨테이너 경계에선 잠금이
전파되지 않아 더 위험 — VERIFICATION 8-2절 실측).

### 5-2. 빈 상태에서 재현 (드라이버 설치 → 연결 → 대시보드)

```bash
cp .env.example .env               # METABASE_ADMIN_PASSWORD를 강한 값으로 채운다
docker compose up -d metabase      # 이미지 빌드에 드라이버 jar 다운로드 포함
.venv/bin/python scripts/metabase_bootstrap.py
```

부트스트랩은 멱등이다(이름으로 찾아 있으면 재사용) — 초기 설정, DuckLake 커넥션,
질문 3개(악화 랭킹 표·일별 추이·악화 쿼리 수), 대시보드 1장 + 인스턴스 필터 배선까지
전부 REST API로 만든다. 손 클릭 재현 절차가 아니라 스크립트가 곧 절차다.

### 5-3. 알려진 제약·주의

- **드라이버-Metabase 버전은 짝이다.** metabase_duckdb_driver 릴리스 이름이
  "Metabase NN + DuckDB x.y.z"다. Metabase 이미지를 올리려면 드라이버도 같이 올릴 것
  (`metabase/Dockerfile`의 ARG 두 개). DuckDB 계열이 dbt 쪽(1.5.x)과 갈라지면
  DuckLake/파일 포맷 호환부터 확인.
- **공식 Metabase 이미지(Alpine)에선 드라이버가 안 뜬다** — glibc 문제. 반드시
  커스텀 이미지로(VERIFICATION 8-1절).
- **커넥션 init_sql에 카탈로그를 건드리는 문장(CREATE SECRET 등)을 넣지 말 것.**
  동시 카드 로딩 때 커넥션 풀이 write-write conflict를 낸다(VERIFICATION 8-6절).
  S3 자격증명은 세션 로컬 `SET s3_*`로.
- 대시보드가 "테이블 없음"이면: `publish` 태스크가 돌았는지부터
  (`docker exec lakehouse-airflow-scheduler python -m extract.publish_marts`로 수동 발행 가능).
- 주간 `ducklake_maintenance` CHECKPOINT는 마트 테이블도 함께 정리한다(발행 커밋이
  스냅샷으로 쌓이므로 정상).

## 6. deadman 감시 (heartbeat) — '미실행'까지 잡는 역방향 경보

### 6-1. 왜

실패 알림(2·4절의 webhook)은 태스크가 **돌다가** 실패해야 운다. 스케줄러가 통째로
죽거나, DAG가 pause되거나, 원천 수집기가 조용히 멈춰 태스크가 **시작조차 안 하면**
on_failure_callback은 불릴 일이 없다 — 아무도 안 운다. 이 구멍은 "성공이 주기적으로
남겨야 할 신호가 안 남았다"는 역방향으로만 잡힌다(9단계).

- `snapshot_offload`의 마지막 태스크 `heartbeat`가 전 단계 성공 시 카탈로그 PG
  (`ducklake_catalog.pipeline_heartbeat`)에 `dag_id, last_success_at`을 upsert한다.
  파일이 아니라 테이블이라 컨테이너가 죽어도 남고 SQL로 조회된다. DBTower 메타 DB는
  건드리지 않는다(5단계부터의 분리).
- `extract/deadman.py`가 그 신호가 **기한 내 갱신됐는가**를 보고, 낡았으면 같은
  webhook 채널로 경보한다. 기본 기한 26h(@daily 24h + 2h 유예).

### 6-2. 두 실행 경로 (하나는 Airflow 안, 하나는 밖)

| 경로 | 무엇을 잡나 | 못 잡는 것 |
|---|---|---|
| Airflow `deadman_watch` DAG (@hourly) | 스케줄러가 사는 동안의 DAG pause·연속 실패·원천 침묵 | **자기 스케줄러의 death**(같이 죽으니까) |
| 외부 `python -m extract.deadman` (host cron/systemd) | 스케줄러가 통째로 죽는 'total death'까지 | 그 외부 러너 자체의 death |

**감시자는 감시 대상 밖에 있어야 total death를 잡는다.** Airflow 안의 감시 DAG만으로는
자기 스케줄러의 죽음을 못 본다 — 그래서 외부 cron 경로를 함께 둔다. 외부 러너 예:

```bash
# crontab -e  (호스트) — 매시 정각, 카탈로그 PG·webhook은 컨테이너 밖에서도 접근 가능해야 함
0 * * * * cd ~/Desktop/dbtower-lakehouse && ALERT_WEBHOOK_URL=<수신기> .venv/bin/python -m extract.deadman
```

- 종료코드 = 경보 발화 수(0=건강, 비영=경보). cron·systemd가 비영 종료를 자체
  경보(메일·OnFailure)로 이중으로 태울 수 있다.
- 감시 대상·기한은 `DEADMAN_WATCH`(예: `"snapshot_offload:26h,ducklake_maintenance:8d"`).

### 6-3. 경보를 받았을 때

`{"event":"pipeline_deadman", "dag_id":..., "age_hours":..., "deadline_hours":...}` 형태다.

1. **age_hours가 있고 deadline 초과** → 마지막 성공(`last_success_at`) 이후 파이프라인이
   안 돌았다. Airflow UI에서 `snapshot_offload` 최근 런 상태를 본다 — 실패면 1절로
   (webhook 실패 알림도 왔을 것), **런 자체가 없으면** DAG가 pause됐거나 스케줄러가
   죽은 것. `docker ps | grep lakehouse-airflow-scheduler`부터.
2. **last_success_at이 null**(heartbeat 없음) → 한 번도 성공 못 했거나 heartbeat
   테이블이 비었다. 새 환경이면 정상(첫 성공 전). 아니면 DAG 미실행/pause 의심.

### 6-4. 배선 점검 (파이프라인 없이)

```bash
# heartbeat 수동 기록 → deadman이 건강으로 보는지
docker exec -w /opt/airflow lakehouse-airflow-scheduler python -m extract.heartbeat snapshot_offload
docker exec -w /opt/airflow lakehouse-airflow-scheduler python -m extract.deadman   # exit 0 = 건강
```

- heartbeat 테이블 직접 조회: `ducklake_catalog` DB의 `pipeline_heartbeat`
  (`SELECT * FROM pipeline_heartbeat`). 원천 메타 DB(dbtower)엔 없다(오염 금지 확인용).

## 7. 주간 운영 보고 이메일 구독 (16단계 G6, 선택)

주간 보고 대시보드(`scripts/metabase_bootstrap.py`가 생성)는 값을 판정 컬럼까지 계산하고
발화는 하지 않는다. 매주 이메일로 받고 싶으면 Metabase 구독(pull→push)을 켠다. 단
**SMTP가 전제**라 셀프호스트 어플라이언스에 강제하지 않는다(메일 서버가 없는 환경도 있으므로).

1. Metabase 관리자 → 설정 → 이메일(SMTP)에서 발신 서버를 등록한다(호스트·포트·계정·발신 주소).
2. "주간 운영 보고" 대시보드 → 우상단 공유 → **구독(Subscription)** → 이메일 → 수신자·주기
   (예: 매주 월 08:00)를 지정한다. 필터(instance 등)를 걸면 그 조건으로 발송된다.
3. 발화 경계 유지: 구독은 "사람이 보는 요약의 정기 배달"이다. 데이터 급변 즉시 알림(D-day·
   breach 발생 순간)은 여기서 하지 않는다 — 그건 DBTower reverse ETL(기계 push)의 몫이다.

SMTP를 안 켜면 대시보드는 그대로 `http://localhost:13001`에서 pull로 본다(구독만 비활성).
