-- 장기 가용성 SLO 마트 (21단계) — "이 인스턴스, 최근 창에서 목표 가용성을 지켰나".
--
-- DBTower는 35일 창으로만 에러버짓을 회계하고 지운다. 이 마트는 최근 N일(기본 30) 가용성을
-- 목표(seed target_pct 또는 var)와 견줘 SRE 에러버짓까지 낸다. 장기 창이라야 가능한 판정 —
-- 30~35일 넘는 분기 가용성은 DBTower도 Prometheus도 안 본다. 발화는 안 한다(13단계 원칙).
--
-- 기종 축(dim_instance) 조인 — "이 DB 어느 기종인데 몇 % 떴나"가 한 줄로 읽힌다.
{{ config(materialized="table") }}

{% set window = var('uptime_window_days', 30) %}
{% set default_target = var('uptime_slo_target', 99.5) %}

with anchor as (
    select max(dt) as max_dt from {{ ref('fct_uptime_daily') }}
),

windowed as (
    select f.*
    from {{ ref('fct_uptime_daily') }} f
    cross join anchor a
    where f.dt >= a.max_dt - {{ window }}
),

agg as (
    select
        instance_id,
        min(dt)                                                  as window_from_dt,
        max(dt)                                                  as window_to_dt,
        count(*)                                                 as days_observed,
        sum(samples)                                             as total_samples,
        sum(down_samples)                                        as total_down_samples,
        round(100.0 * sum(up_samples) / nullif(sum(samples), 0), 3) as window_uptime_pct,
        min(uptime_pct)                                          as worst_day_uptime_pct,
        min_by(dt, uptime_pct)                                   as worst_day,
        round(avg(avg_ping_ms), 1)                               as avg_ping_ms
    from windowed
    group by instance_id
)

select
    a.instance_id,
    di.instance_name,
    di.engine,
    a.window_from_dt,
    a.window_to_dt,
    a.days_observed,
    a.total_samples,
    a.total_down_samples,
    a.window_uptime_pct,
    a.worst_day_uptime_pct,
    a.worst_day,
    a.avg_ping_ms,
    coalesce(t.target_pct, {{ default_target }})                 as target_pct,
    -- SRE 에러버짓 잔량: 허용 다운타임 중 얼마가 남았나. 음수면 목표 초과(breach).
    -- target=100이면 어떤 다운도 소진이라 정의 불가(NULL).
    round(100.0 * (1 - (100 - a.window_uptime_pct)
          / nullif(100 - coalesce(t.target_pct, {{ default_target }}), 0)), 1) as error_budget_remaining_pct,
    case
        when a.window_uptime_pct < coalesce(t.target_pct, {{ default_target }}) then 'breach'
        when 100.0 * (1 - (100 - a.window_uptime_pct)
             / nullif(100 - coalesce(t.target_pct, {{ default_target }}), 0)) < 20 then 'at_risk'
        else 'meets'
    end                                                          as slo_status,
    current_timestamp                                            as computed_at
from agg a
left join {{ ref('dim_instance') }} di on a.instance_id = di.instance_id
left join {{ ref('uptime_slo_targets') }} t on a.instance_id = t.instance_id
