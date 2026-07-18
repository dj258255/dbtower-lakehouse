-- 변경 영향 상관 마트 (18단계 신설·19 지연축·20 볼륨축) — 이 창고만 할 수 있는 "원인 후보 지목".
--
-- 변경 이벤트(int_change_events)와 성능 신호를 시간축으로 겹친다: 어떤 변경 뒤 N일(기본 7)
-- 안에 그 인스턴스에서 (a) 플랜이 뒤집히거나 회귀했나, (b) 평균 지연이 올랐나, (c) 용량이
-- 급증했나. 장기 변경 이력과 장기 성능 이력이 같은 창고에 있어야 가능 — DBTower 7일은 못 한다.
--
-- 축을 셋으로 넓혔다: 플랜(뒤집힘/회귀) → 지연(19단계) → 볼륨(20단계, 용량 급증). 원인의 갈래가
-- 여럿이라 한 축만 보면 놓친다. 볼륨 축은 인과가 특히 약하다(설정이 디스크를 직접 키우는 경우는
-- 드묾 — 로깅·retention 파라미터 정도) — 그래서 더더욱 조언 어휘로만 싣는다.
--
-- **상관은 인과가 아니다.** 관측 사실만 우선순위로 싣고 최종 판단은 사람. 지연·볼륨 모두 후행
-- 관측이 필요해 당일 변경은 아직 no_signal이 정답(시간이 해제). 이름은 config뿐이라 정확.
{{ config(materialized="table") }}

{% set n = var('change_impact_window_days', 7) %}
{% set lat_ratio = var('change_impact_latency_ratio', 1.3) %}
{% set size_ratio = var('change_impact_size_ratio', 1.1) %}

with changes as (

    select instance_id, change_dt, change_source, change_key, change_kind, old_value, new_value
    from {{ ref('int_change_events') }}

),

flips as (

    select instance_id, flip_dt, verdict from {{ ref('mart_plan_regression') }}

),

-- 지연·볼륨은 (instance, dt) 단위로 먼저 접어 조인 카티전을 줄인다(평균/합 의미는 불변).
lat as (

    select instance_id, dt, avg(avg_latency_ms) as day_latency_ms
    from {{ ref('fct_query_daily') }}
    where avg_latency_ms is not null
    group by instance_id, dt

),

size_daily as (

    select instance_id, dt, sum(total_bytes) as day_bytes
    from {{ ref('fct_size_daily') }}
    group by instance_id, dt

),

joined as (

    select
        c.instance_id,
        c.change_dt,
        c.change_source,
        c.change_key,
        c.change_kind,
        c.old_value,
        c.new_value,
        count(distinct f.flip_dt)                                            as plan_flips_after,
        count(distinct f.flip_dt) filter (f.verdict = 'REGRESSED')           as regressed_after,
        avg(case when l.dt between c.change_dt - {{ n }} and c.change_dt - 1
                 then l.day_latency_ms end)                                   as before_latency_ms,
        avg(case when l.dt between c.change_dt + 1 and c.change_dt + {{ n }}
                 then l.day_latency_ms end)                                   as after_latency_ms,
        avg(case when s.dt between c.change_dt - {{ n }} and c.change_dt - 1
                 then s.day_bytes end)                                        as before_bytes,
        avg(case when s.dt between c.change_dt + 1 and c.change_dt + {{ n }}
                 then s.day_bytes end)                                        as after_bytes
    from changes c
    left join flips f
        on  f.instance_id = c.instance_id
        and f.flip_dt between c.change_dt and c.change_dt + {{ n }}
    left join lat l
        on  l.instance_id = c.instance_id
        and l.dt between c.change_dt - {{ n }} and c.change_dt + {{ n }}
    left join size_daily s
        on  s.instance_id = c.instance_id
        and s.dt between c.change_dt - {{ n }} and c.change_dt + {{ n }}
    group by 1, 2, 3, 4, 5, 6, 7

)

select
    instance_id,
    change_dt,
    change_source,
    change_key,
    change_kind,
    old_value,
    new_value,
    plan_flips_after,
    regressed_after,
    round(before_latency_ms, 2)                                              as before_latency_ms,
    round(after_latency_ms, 2)                                               as after_latency_ms,
    round(after_latency_ms / nullif(before_latency_ms, 0), 2)                as latency_ratio,
    before_bytes,
    after_bytes,
    round(after_bytes / nullif(before_bytes, 0), 2)                          as size_ratio,
    -- 우선순위: 회귀(가장 강) > 플랜 뒤집힘 > 지연 상승 > 볼륨 급증(인과 약함) > 신호 없음.
    case
        when regressed_after > 0  then 'followed_by_regression'
        when plan_flips_after > 0 then 'followed_by_plan_flip'
        when before_latency_ms is not null and after_latency_ms is not null
             and round(after_latency_ms / nullif(before_latency_ms, 0), 2) >= {{ lat_ratio }}
            then 'followed_by_latency_rise'
        when before_bytes is not null and after_bytes is not null
             and round(after_bytes / nullif(before_bytes, 0), 2) >= {{ size_ratio }}
            then 'followed_by_size_growth'
        else 'no_signal'
    end                                                                      as correlation,
    current_timestamp                                                        as computed_at
from joined
