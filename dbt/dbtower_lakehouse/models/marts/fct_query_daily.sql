-- 일간 발생량 팩트(marts).
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
