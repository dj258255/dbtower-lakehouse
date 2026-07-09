-- 롤링 윈도우 악화 쿼리 랭킹(marts) — Phase 10 재설계.
--
-- 0편이 답 못했던 질문은 "이번 달 vs 지난달 느려진 쿼리 있어?"였다. 그런데 Phase 7의
-- 이 마트는 "전체 이력 첫 활동일 vs 마지막 활동일"을 비교했다 — 이력이 3일일 땐
-- 그럴듯했지만, 적재가 1년 쌓이면 "1년 전 대비 지금"이 되어 버린다(README의 '지난달
-- 대비'와 어긋남). 365dt 규모 실측에서 이 어긋남이 수치로 드러났다.
--
-- 재설계: 고정된 첫날/마지막날이 아니라, 데이터의 최신 dt를 기준으로 한
--   최근 recent_days일  vs  그 직전 prior_days일
-- 의 **롤링 윈도우** 평균 지연을 비교한다. 이력이 아무리 길어져도 창은 항상
-- "최근 대 직전"으로 미끄러진다. 창 크기는 dbt var로 조정 가능(기본 7 vs 30).
--
-- 잡음 억제: 하루 delta_calls가 임계 미만인 날은 평균 지연이 불안정하므로 제외.
-- 두 창 모두에서 최소 하루는 관측돼야 비교한다(한쪽만 있으면 비교 불가 → 제외).
{% set recent_days = var('regression_recent_days', 7) %}
{% set prior_days = var('regression_prior_days', 30) %}
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

anchor as (

    select max(dt) as max_dt from daily

),

-- 최신 dt 기준으로 각 행을 recent / prior / 그 밖으로 라벨링.
--   recent = (max_dt - recent_days, max_dt]
--   prior  = (max_dt - recent_days - prior_days, max_dt - recent_days]
-- DATE - 정수 = DATE (DuckDB) — 경계 산수를 정수 일수로 안정적으로 한다.
windowed as (

    select
        d.*,
        case
            when d.dt >  a.max_dt - {{ recent_days }}                 then 'recent'
            when d.dt >  a.max_dt - {{ recent_days + prior_days }}    then 'prior'
        end as win
    from daily d
    cross join anchor a

),

agg as (

    select
        instance_id,
        query_id,
        any_value(query_text)                                                   as query_text,
        avg(case when win = 'recent' then avg_latency_ms end)                    as recent_latency_ms,
        avg(case when win = 'prior'  then avg_latency_ms end)                    as prior_latency_ms,
        sum(case when win = 'recent' then delta_calls end)                       as recent_delta_calls,
        count(distinct case when win = 'recent' then dt end)                     as recent_days_seen,
        count(distinct case when win = 'prior'  then dt end)                     as prior_days_seen,
        min(case when win = 'recent' then dt end)                               as recent_from_dt,
        max(case when win = 'recent' then dt end)                               as recent_to_dt
    from windowed
    where win is not null
    group by instance_id, query_id

)

select
    instance_id,
    query_id,
    query_text,
    recent_from_dt,
    recent_to_dt,
    recent_days_seen,
    prior_days_seen,
    prior_latency_ms,
    recent_latency_ms,
    recent_latency_ms - prior_latency_ms                                            as latency_increase_ms,
    round(100.0 * (recent_latency_ms - prior_latency_ms) / nullif(prior_latency_ms, 0), 1) as latency_increase_pct,
    recent_delta_calls
from agg
where recent_days_seen  >= 1                 -- 최근 창에 관측 있어야
  and prior_days_seen   >= 1                 -- 직전 창에도 있어야(비교 가능)
  and recent_latency_ms  > prior_latency_ms  -- 악화된 것만(개선은 별도 관심사)
order by latency_increase_ms desc
