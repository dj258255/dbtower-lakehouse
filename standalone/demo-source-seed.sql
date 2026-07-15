-- 데모 원천 시드 (--profile demo, 첫 부팅 1회).
--
-- 내 DBTower 없이도 전체 파이프라인을 e2e로 돌리기 위한 최소 원천.
-- CONTRACT §1의 두 테이블을 그대로 재현한다:
--   database_instance : 레지스트리(offload 루프 드라이버 — 이게 없으면 조용히 빈 결과)
--   query_snapshot    : 팩트(누적 카운터 스냅샷)
-- 데이터는 scripts/ci_fixture.py의 _ROWS와 동일 — 델타 로직의 대표 경로를 담는다
-- (정상 증가 · 지문 충돌(같은 instance/query/captured_at 2행) · 순리셋).

CREATE TABLE database_instance (
    id      BIGINT PRIMARY KEY,
    name    VARCHAR(200),
    engine  VARCHAR(50)
);

INSERT INTO database_instance (id, name, engine) VALUES
    (1, 'demo-mysql-01',    'MYSQL'),
    (2, 'demo-postgres-01', 'POSTGRESQL');

CREATE TABLE query_snapshot (
    id             BIGINT PRIMARY KEY,
    instance_id    BIGINT NOT NULL,
    captured_at    TIMESTAMP NOT NULL,
    query_id       VARCHAR(64) NOT NULL,
    query_text     VARCHAR(4000),
    calls          BIGINT NOT NULL,
    total_time_ms  DOUBLE PRECISION NOT NULL,
    rows_examined  BIGINT NOT NULL
);

-- 원천 인덱스 — offload가 (instance_id, captured_at) 선두를 타는 전제(CONTRACT §1).
CREATE INDEX idx_snapshot_instance_time ON query_snapshot (instance_id, captured_at);

-- inst 1 · q1 — 이틀에 걸쳐 악화. 12:00에 지문 충돌 2행(staging SUM 대상).
INSERT INTO query_snapshot (id, instance_id, captured_at, query_id, query_text, calls, total_time_ms, rows_examined) VALUES
    (1, 1, '2026-01-01 06:00:00', 'q1', 'SELECT 1',    0,     0.0,     0),
    (2, 1, '2026-01-01 12:00:00', 'q1', 'SELECT 1',  120,  1200.0,  1200),
    (3, 1, '2026-01-01 12:00:00', 'q1', 'SELECT 1',   80,   800.0,   800),
    (4, 1, '2026-01-01 23:00:00', 'q1', 'SELECT 1',  200,  2000.0,  2000),
    (5, 1, '2026-01-02 06:00:00', 'q1', 'SELECT 1', 1000, 20000.0, 20000),
    (6, 1, '2026-01-02 23:00:00', 'q1', 'SELECT 1', 1300, 29000.0, 29000),
-- inst 2 · q2 — 01-01 하루 중 카운터 리셋(last<first → 델타 0 클램프), 01-02 정상 증가.
    (7, 2, '2026-01-01 06:00:00', 'q2', 'SELECT 2',  500,  5000.0,  5000),
    (8, 2, '2026-01-01 23:00:00', 'q2', 'SELECT 2',  100,  1000.0,  1000),
    (9, 2, '2026-01-02 06:00:00', 'q2', 'SELECT 2',  100,  1000.0,  1000),
    (10, 2, '2026-01-02 23:00:00', 'q2', 'SELECT 2',  400, 12000.0, 12000);
