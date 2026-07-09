-- 일간 발생량 팩트(marts).
--
-- 증분(incremental) — Phase 10. 365dt 규모 실측에서 전체 재빌드(table)가 407s로
-- 드러났다(fct는 매일 O(전체 이력)을 다시 계산). 그런데 이 팩트의 grain은 dt 단위로
-- **완전히 독립**이다 — 하루 발생량은 그날 파티션 양 끝(first/last)의 차분이라 다른
-- 날짜와 섞이지 않는다. 그래서 새 dt만 계산해 append/replace 하면 결과가 같다.
-- delete+insert 전략 + unique_key=(instance_id,query_id,dt): 새 dt는 순수 insert,
-- 같은 dt 재실행(당일 재시도·정정)은 그 dt만 삭제 후 재삽입(멱등 유지). 과거 dt(<max)
-- 정정은 --full-refresh가 필요하다(backfill 레시피 — docs/RUNBOOK.md).
{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["instance_id", "query_id", "dt"],
        on_schema_change="fail",
    )
}}
--
-- calls/total_time_ms는 누적 카운터다. 그냥 SUM 하면 무의미하고, 하루의 실제
-- 발생량은 하루 구간 '양 끝'의 차분이다. 이는 DBTower ComparisonService의
--   Math.max(0, end.getCalls() - start.getCalls())
-- 와 정확히 같은 원리다. 같은 방식을 쓰면 DBTower 화면의 시점 비교와 교차검증도 된다.
--
-- 리셋 클램프: 대상 DB가 하루 중 재기동하면 카운터가 리셋되어 last < first가 될 수
-- 있다(순리셋 그레인 관측됨). 그때 음수 델타는 GREATEST(0, ...)로 0에 클램프한다.
--
-- 왜 first-vs-last인가(스냅샷 간 양의 델타 합산 대신):
--   staging에서 지문 충돌을 SUM으로 접었기 때문에, 어떤 쿼리가 스냅샷 사이에
--   보고됐다 안 됐다 하면 합계가 출렁인다. 인접 델타를 합산하면 그 '유령 재등장'을
--   활동으로 과대계상한다(실측 22.3M vs 3.1M). 하루 양 끝만 보는 first-vs-last는
--   중간 출렁임에 흔들리지 않고, DBTower 정식 로직과도 일치한다.
with snap as (

    select
        instance_id,
        query_id,
        dt,
        captured_at,
        calls,
        total_time_ms,
        query_text
    from {{ ref('stg_query_snapshot') }}
    {% if is_incremental() %}
    -- 워터마크(현재 fct의 max dt)를 컴파일 타임에 리터럴로 구워 넣는다. 스칼라 서브쿼리
    -- (select max(dt) from this)로는 DuckDB가 hive 파티션 프루닝을 못 해 2190개 파일을
    -- 전부 스캔했다(실측: 규모에서 여전히 느림). 리터럴 상수면 파티션 경로 프루닝이
    -- 걸려 최신 dt의 파일만 읽는다 — 이게 증분의 실제 이득이다.
    -- >= 라 최신 dt 재실행도 delete+insert로 멱등하게 갱신된다.
    {% set max_dt = run_query("select max(dt) from " ~ this).columns[0].values()[0] %}
    where dt >= '{{ max_dt }}'
    {% endif %}

),

endpoints as (

    select
        instance_id,
        query_id,
        dt,
        query_text,
        calls,
        total_time_ms,
        row_number() over (partition by instance_id, query_id, dt order by captured_at asc)  as rn_first,
        row_number() over (partition by instance_id, query_id, dt order by captured_at desc) as rn_last
    from snap

),

diffed as (

    select
        instance_id,
        query_id,
        dt,
        any_value(query_text)                                          as query_text,
        max(case when rn_first = 1 then calls end)                     as first_calls,
        max(case when rn_last  = 1 then calls end)                     as last_calls,
        max(case when rn_first = 1 then total_time_ms end)             as first_time_ms,
        max(case when rn_last  = 1 then total_time_ms end)             as last_time_ms
    from endpoints
    group by instance_id, query_id, dt

)

select
    instance_id,
    query_id,
    dt,
    query_text,
    greatest(0, last_calls - first_calls)                          as delta_calls,
    greatest(0, last_time_ms - first_time_ms)                      as delta_total_time_ms,
    greatest(0, last_time_ms - first_time_ms)
        / nullif(greatest(0, last_calls - first_calls), 0)         as avg_latency_ms
from diffed
