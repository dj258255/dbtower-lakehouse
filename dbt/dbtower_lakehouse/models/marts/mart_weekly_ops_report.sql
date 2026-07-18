-- 주간 운영 보고 마트 (16단계 G5) — 네 판정을 한 장으로 접는다.
--
-- 현업 DBA 시간을 제일 많이 먹는 건 장애가 아니라 보고서다. 용량 D-day·top 대기·플랜
-- 뒤집힘·백업 공백을 매주 사람이 화면 네 곳에서 긁어모으던 걸, 이미 있는 마트 넷의 조합으로
-- 인스턴스당 한 행으로 만든다. 새 계산은 거의 없다(서빙 편의 마트).
--
-- 관심사·시점 정직 표기:
--  · 용량(capacity)·대기(wait)·백업(backup)은 "지금 시점(최신 dt)" 스냅샷 마트다 — 과거 주로
--    거슬러 재구성하지 않는다(원천 마트가 현재 상태만 보유). 그래서 이 보고는 **가장 최근 주**
--    한 장이다.
--  · 플랜 뒤집힘만 flip_dt가 있어 주 단위로 집계 가능 — 이번 주 발생분만 센다.
--  · 이력이 한 주가 안 차면 is_partial_week=true로 표시해, 소비자가 반쪽 주를 온전한 주와
--    비교하지 않게 한다.
{{ config(materialized="table") }}

with anchor as (

    select max(dt) as max_dt from {{ ref('fct_query_daily') }}

),

week_bounds as (

    -- 주 경계는 UTC date_trunc('week', ...)(dow×hour 베이스라인과 같은 결 — 표시만 로컬).
    select
        cast(date_trunc('week', max_dt) as date)                      as week_start,
        cast(date_trunc('week', max_dt) + interval 6 day as date)     as week_end,
        max_dt
    from anchor

),

universe as (

    select distinct instance_id from {{ ref('fct_query_daily') }}

),

cap as (

    -- 인스턴스당 가장 임박한 D-day와 최악 위험도. 위험도는 문자 정렬 대신 명시 우선순위로.
    select
        instance_id,
        min(days_to_threshold)                                              as min_days_to_threshold,
        min(case risk_flag
                when 'd30' then 1 when 'd90' then 2 when 'growth_only' then 3
                when 'ok' then 4 when 'stable_or_shrinking' then 5
                when 'learning' then 6 else 9 end)                          as worst_rank
    from {{ ref('mart_capacity_forecast') }}
    group by instance_id

),

wait_top1 as (

    select
        instance_id,
        event_name       as top_wait_event,
        total_delta_ms   as top_wait_delta_ms
    from {{ ref('mart_wait_top') }}
    where rank_in_instance = 1

),

plan_wk as (

    select
        p.instance_id,
        count(*)                                        as plan_flips_this_week,
        count(*) filter (p.verdict = 'REGRESSED')       as plan_regressed_this_week
    from {{ ref('mart_plan_regression') }} p
    cross join week_bounds w
    where p.flip_dt between w.week_start and w.week_end
    group by p.instance_id

),

backup as (

    select instance_id, gap_days as backup_gap_days, rpo_status as backup_status
    from {{ ref('mart_backup_rpo') }}

),

uptime_wk as (

    select
        f.instance_id,
        round(100.0 * sum(f.up_samples) / nullif(sum(f.samples), 0), 2) as this_week_uptime_pct
    from {{ ref('fct_uptime_daily') }} f
    cross join week_bounds w
    where f.dt between w.week_start and w.week_end
    group by f.instance_id

)

select
    w.week_start,
    w.week_end,
    (w.max_dt < w.week_end)                                                 as is_partial_week,
    u.instance_id,
    di.instance_name,
    di.engine,
    cap.min_days_to_threshold,
    case cap.worst_rank
        when 1 then 'd30' when 2 then 'd90' when 3 then 'growth_only'
        when 4 then 'ok' when 5 then 'stable_or_shrinking'
        when 6 then 'learning' else null end                               as capacity_worst_risk,
    wt.top_wait_event,
    wt.top_wait_delta_ms,
    coalesce(pw.plan_flips_this_week, 0)                                    as plan_flips_this_week,
    coalesce(pw.plan_regressed_this_week, 0)                               as plan_regressed_this_week,
    b.backup_gap_days,
    b.backup_status,
    uw.this_week_uptime_pct,
    current_timestamp                                                       as computed_at
from universe u
cross join week_bounds w
left join {{ ref('dim_instance') }} di on u.instance_id = di.instance_id
left join cap       on u.instance_id = cap.instance_id
left join wait_top1 wt on u.instance_id = wt.instance_id
left join plan_wk   pw on u.instance_id = pw.instance_id
left join backup    b  on u.instance_id = b.instance_id
left join uptime_wk uw on u.instance_id = uw.instance_id
