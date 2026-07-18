-- 설정 변경 영향 상관 마트 (18단계) — 이 창고만 할 수 있는 "원인 후보 지목".
--
-- 설정 변경 이벤트와 플랜 뒤집힘(mart_plan_regression)을 시간축으로 겹친다: 어떤 파라미터가
-- 바뀐 뒤 N일(기본 7) 안에 그 인스턴스에서 플랜이 뒤집히거나 회귀(REGRESSED)가 관측됐나.
-- 장기 설정 이력 + 장기 성능 이력이 같은 창고에 있어야 가능한 판정 — DBTower 7일 창은 못 한다.
--
-- **상관은 인과가 아니다.** "이 설정이 원인"이라 단정하지 않고 "변경 뒤 회귀가 뒤따랐다"까지만
-- 조언 어휘로 싣는다(mart_index_verdict의 정직 어휘와 같은 결). 최종 판단은 사람의 몫.
-- 플랜 이력이 얕으면(초기) 대부분 no_flip_observed가 정직한 결과 — 이력이 쌓이며 켜진다.
{{ config(materialized="table") }}

{% set n = var('config_impact_window_days', 7) %}

with changes as (

    select
        instance_id,
        dt as change_dt,
        param_name,
        old_value,
        new_value,
        change_kind
    from {{ ref('stg_config_param_change') }}

),

flips as (

    select instance_id, flip_dt, verdict
    from {{ ref('mart_plan_regression') }}

),

joined as (

    -- 변경일부터 +N일 안의 뒤집힘을 센다. 변경 당일 포함(그날 이후 회귀는 변경과 상관 후보).
    select
        c.instance_id,
        c.change_dt,
        c.param_name,
        c.old_value,
        c.new_value,
        c.change_kind,
        count(f.flip_dt)                                    as plan_flips_after,
        count(f.flip_dt) filter (f.verdict = 'REGRESSED')   as regressed_after
    from changes c
    left join flips f
        on  f.instance_id = c.instance_id
        and f.flip_dt between c.change_dt and c.change_dt + {{ n }}
    group by 1, 2, 3, 4, 5, 6

)

select
    instance_id,
    change_dt,
    param_name,
    old_value,
    new_value,
    change_kind,
    plan_flips_after,
    regressed_after,
    case
        when regressed_after > 0  then 'followed_by_regression'
        when plan_flips_after > 0 then 'followed_by_plan_flip'
        else 'no_flip_observed'
    end                                                     as correlation,
    current_timestamp                                       as computed_at
from joined
