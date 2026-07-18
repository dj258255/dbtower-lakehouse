-- 변경 영향 상관 마트 (18단계 신설·19단계 축 확장) — 이 창고만 할 수 있는 "원인 후보 지목".
--
-- 변경 이벤트(int_change_events)와 성능 신호를 시간축으로 겹친다: 어떤 변경 뒤 N일(기본 7)
-- 안에 그 인스턴스에서 (a) 플랜이 뒤집히거나 회귀했나, (b) 평균 지연이 올랐나. 장기 변경 이력과
-- 장기 성능 이력이 같은 창고에 있어야 가능한 판정 — DBTower 7일 창은 못 한다.
--
-- 19단계 확장: 플랜 뒤집힘 하나만 보던 걸 **지연 전후 비교**까지 넓혔다. 플랜이 안 뒤집혀도
-- "변경 뒤 느려졌다"를 잡는다(플랜 이력이 얕아도 켜지는 축). 변경 소스도 int_change_events로
-- 일반화 — 지금은 config, 스키마 변경(change_review)은 자리만 열려 있다.
--
-- **상관은 인과가 아니다.** "이 변경이 원인"이라 단정하지 않고 관측 사실만 조언 어휘로 싣는다
-- (mart_index_verdict와 같은 결). 최종 판단은 사람의 몫. 이름은 mart_config_impact 유지 —
-- 현재 소스가 config뿐이라 정확하다(스키마 변경이 실제로 편입될 때 일반명으로 개명).
{{ config(materialized="table") }}

{% set n = var('change_impact_window_days', 7) %}
{% set lat_ratio = var('change_impact_latency_ratio', 1.3) %}

with changes as (

    select
        instance_id,
        change_dt,
        change_source,
        change_key,
        change_kind,
        old_value,
        new_value
    from {{ ref('int_change_events') }}

),

flips as (

    select instance_id, flip_dt, verdict
    from {{ ref('mart_plan_regression') }}

),

lat as (

    select instance_id, dt, avg_latency_ms
    from {{ ref('fct_query_daily') }}
    where avg_latency_ms is not null

),

joined as (

    -- 두 신호를 한 번에 붙인다. flips×lat 카티전이 생기지만 count(distinct flip_dt)는 뒤집힘을
    -- 정확히 세고, avg(case ... lat)은 중복돼도 평균이 불변이라 지연 전후도 정확하다.
    -- 지연 NULL은 avg가 자연 제외(delta_calls=0인 날을 0으로 접지 않음 — 회귀↔개선 오판 방지).
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
                 then l.avg_latency_ms end)                                   as before_latency_ms,
        avg(case when l.dt between c.change_dt + 1 and c.change_dt + {{ n }}
                 then l.avg_latency_ms end)                                   as after_latency_ms
    from changes c
    left join flips f
        on  f.instance_id = c.instance_id
        and f.flip_dt between c.change_dt and c.change_dt + {{ n }}
    left join lat l
        on  l.instance_id = c.instance_id
        and l.dt between c.change_dt - {{ n }} and c.change_dt + {{ n }}
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
    -- 우선순위: 회귀(가장 강한 신호) > 플랜 뒤집힘 > 지연 상승 > 신호 없음.
    case
        when regressed_after > 0  then 'followed_by_regression'
        when plan_flips_after > 0 then 'followed_by_plan_flip'
        when before_latency_ms is not null and after_latency_ms is not null
             and round(after_latency_ms / nullif(before_latency_ms, 0), 2) >= {{ lat_ratio }}
            then 'followed_by_latency_rise'
        else 'no_signal'
    end                                                                      as correlation,
    current_timestamp                                                        as computed_at
from joined
