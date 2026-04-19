-- 구간 대비 가장 악화된 쿼리 랭킹(marts).
--
-- 0편이 답 못했던 질문 — "지난 구간보다 느려진 쿼리 있어?" — 에 답하는 모델.
-- raw(7일 보존)만으로는 구간 비교가 불가능하지만, fct_query_daily가 만든
-- 일간 avg_latency_ms를 인스턴스+쿼리별로 첫 활동일 vs 마지막 활동일로 비교한다.
--
-- 잡음 억제: 하루 delta_calls가 임계(min_calls) 미만인 날은 평균 지연이 불안정하므로
-- 비교 대상에서 뺀다.
{% set min_calls = 100 %}

with daily as (

    select
        instance_id,
        query_id,
        dt,
        query_text,
        delta_calls,
        avg_latency_ms
    from {{ ref('fct_query_daily') }}
    where delta_calls >= {{ min_calls }}
      and avg_latency_ms is not null

),

ranked as (

    select
        *,
        row_number() over (partition by instance_id, query_id order by dt asc)  as rn_first,
        row_number() over (partition by instance_id, query_id order by dt desc) as rn_last
    from daily

),

bounds as (

    select
        instance_id,
        query_id,
        max(case when rn_first = 1 then dt end)             as first_dt,
        max(case when rn_last  = 1 then dt end)             as last_dt,
        max(case when rn_first = 1 then avg_latency_ms end) as first_latency_ms,
        max(case when rn_last  = 1 then avg_latency_ms end) as last_latency_ms,
        max(case when rn_last  = 1 then delta_calls end)    as last_delta_calls,
        max(case when rn_last  = 1 then query_text end)     as query_text
    from ranked
    group by instance_id, query_id

)

select
    instance_id,
    query_id,
    query_text,
    first_dt,
    last_dt,
    first_latency_ms,
    last_latency_ms,
    last_latency_ms - first_latency_ms                                              as latency_increase_ms,
    round(100.0 * (last_latency_ms - first_latency_ms) / nullif(first_latency_ms, 0), 1) as latency_increase_pct,
    last_delta_calls
from bounds
where last_dt > first_dt              -- 최소 이틀 이상 관측된 쿼리만
  and last_latency_ms > first_latency_ms  -- 악화된 것만(개선은 별도 관심사)
order by latency_increase_ms desc
