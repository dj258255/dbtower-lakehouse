# RUNBOOK — 장애 대응·backfill·유지보수 절차

> 파이프라인은 언젠가 실패한다. 문서 없는 파이프라인은 실패하는 날 만든 사람의 기억력
> 테스트가 된다. 이 문서는 "새벽에 알림을 받은 사람"이 처음부터 끝까지 따라갈 수 있는
> 절차를 못박는다. 모든 명령은 실측으로 검증했다(docs/VERIFICATION.md 7절).

## 0. 구성 요약

| 구성 | 위치 |
|---|---|
| DAG | `snapshot_offload`(@daily, offload → quality_gate → transform) · `ducklake_maintenance`(@weekly) |
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
자동 실행**하는 걸 막기 위해서다(Phase 0 결정). 과거 날짜는 아래처럼 **명시적으로만** 돌린다.

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
  `ArchiveSelfDestructError`로 시끄럽게 실패한다(Phase 8 가드). 원천이 0행인데
  기존 parquet 파티션이 존재하면 그 파티션이 **유일본**일 수 있어 삭제를 거부하는
  것 — 파티션은 그대로 보존되고 알림(webhook)이 온다. 이 에러가 오면 재시도하지
  말고 dt를 다시 확인하라. 정말 지워야 하는 파티션이면 사람이 MinIO에서 명시적으로
  지운 뒤 재실행한다.

### 2-3. 검증

```bash
.venv/bin/python -m extract.quality 2026-07-06     # 게이트 4검문 PASS 확인
```

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
- **대상**: 카탈로그에 지금 존재하는 테이블 전체(마트 포함)를 잰다(Phase 8 — 특정
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
