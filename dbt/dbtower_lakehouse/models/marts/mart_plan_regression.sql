-- 플랜 회귀 판정 마트 (16단계 G1~G3) — "옵티마이저가 플랜을 갈아탔는데 그게 더 느린가".
--
-- 0편이 답 못했던 새벽 장애의 단골: 통계 갱신·분포 변화로 실행 플랜이 뒤집혔는데 새 플랜이
-- 더 느린 경우. 창고엔 plan_snapshot(플랜이 언제 바뀌었나)과 fct_query_daily(그 쿼리가 실제로
-- 느려졌나)가 둘 다 있는데 서로 몰랐다 — 이 마트가 둘을 시간축으로 상관시킨다. "며칠치 전후
-- 비교"라 라이브 7일 창(DBTower)이 아니라 장기 창고의 몫이다.
--
-- 발화 주체(13단계 확정): 판정 컬럼(verdict)까지만 계산한다 — 알림은 안 쏜다. 소비는
-- Metabase(사람, pull) 또는 DBTower(기계, push)의 몫. 두 번째 알림 시스템을 만들지 않는다.
--
-- 정직한 한계:
--  · lag 기반 뒤집힘은 **창고에 남은 이력** 기준이다. plan_snapshot 원천 보존이 카운트+48h
--    하한(DBTower D2)이라, 적재 이전 과거의 "직전 플랜"은 이미 지워졌을 수 있다 → 초기엔
--    PENDING이 많고, 이력이 쌓일수록 확정된다.
--  · 볼륨 성장으로 같은 플랜이 느려지는 회귀는 이 마트가 아니라 mart_query_regression(롤링
--    랭킹)의 관심사다 — 원인 축이 다르다(플랜 변경 vs 볼륨 성장).
{{ config(materialized="table") }}

{% set n = var('plan_regress_window_days', 3) %}
{% set default_ratio = var('plan_regress_ratio', 1.3) %}
{% set default_min_calls = var('plan_regress_min_calls', 100) %}

with daily_plan as (

    -- 하루의 대표 플랜 = 그날 마지막 관측의 plan_hash(fct_size_daily의 '마지막이 대푯값'과
    -- 같은 결). 하루 안 여러 스냅샷의 출렁임을 접어, 일 단위 grain으로 fct_query_daily와 맞춘다.
    select
        instance_id,
        query_id,
        dt,
        max_by(plan_hash, captured_at) as plan_hash
    from {{ ref('stg_plan_snapshot') }}
    group by instance_id, query_id, dt

),

flips_raw as (

    -- 직전 대표 플랜과 비교. 같은 해시 반복은 뒤집힘이 아니고, 첫 관측(직전 없음)은 등장이지
    -- 뒤집힘이 아니다 — 둘 다 아래 where에서 걸러진다.
    select
        instance_id,
        query_id,
        dt                                                                     as flip_dt,
        plan_hash                                                              as new_plan_hash,
        lag(plan_hash) over (partition by instance_id, query_id order by dt)    as prev_plan_hash
    from daily_plan

),

flips as (

    select
        instance_id,
        query_id,
        flip_dt,
        prev_plan_hash,
        new_plan_hash,
        -- 같은 쿼리의 인접 뒤집힘 — 비교창(±N일) 오염 판정용(G3).
        lag(flip_dt)  over (partition by instance_id, query_id order by flip_dt) as prev_flip_dt,
        lead(flip_dt) over (partition by instance_id, query_id order by flip_dt) as next_flip_dt
    from flips_raw
    where prev_plan_hash is not null          -- 첫 등장은 뒤집힘 아님
      and new_plan_hash <> prev_plan_hash      -- 해시가 실제로 바뀐 것만

),

compared as (

    -- 뒤집힘 전 N일 vs 후 N일 평균 지연. 뒤집힘 당일(flip_dt)은 전/후 플랜이 섞이므로
    -- 양쪽에서 제외한다. avg(case ...)는 NULL을 자연 제외 — delta_calls=0인 날의 지연이
    -- NULL(0 아님, fct_query_daily의 nullif 설계)이라, NULL을 0으로 접어 회귀를 개선으로
    -- 오판하는 함정을 피한다.
    select
        f.instance_id,
        f.query_id,
        f.flip_dt,
        f.prev_plan_hash,
        f.new_plan_hash,
        f.prev_flip_dt,
        f.next_flip_dt,
        avg(case when q.dt between f.flip_dt - {{ n }} and f.flip_dt - 1
                 then q.avg_latency_ms end)                                      as before_avg_ms,
        avg(case when q.dt between f.flip_dt + 1 and f.flip_dt + {{ n }}
                 then q.avg_latency_ms end)                                      as after_avg_ms,
        count(distinct case when q.dt between f.flip_dt - {{ n }} and f.flip_dt - 1
                             and q.avg_latency_ms is not null then q.dt end)     as before_days,
        count(distinct case when q.dt between f.flip_dt + 1 and f.flip_dt + {{ n }}
                             and q.avg_latency_ms is not null then q.dt end)     as after_days,
        sum(case when q.dt between f.flip_dt + 1 and f.flip_dt + {{ n }}
                 then q.delta_calls end)                                         as after_calls
    from flips f
    left join {{ ref('fct_query_daily') }} q
        on  f.instance_id = q.instance_id
        and f.query_id    = q.query_id
        and q.dt between f.flip_dt - {{ n }} and f.flip_dt + {{ n }}
    group by 1, 2, 3, 4, 5, 6, 7

),

judged as (

    select
        c.*,
        (
            (c.prev_flip_dt is not null and c.flip_dt - c.prev_flip_dt <= {{ n }})
            or (c.next_flip_dt is not null and c.next_flip_dt - c.flip_dt <= {{ n }})
        )                                                                        as window_contaminated,
        -- 비율은 표시 정밀도(소수 2자리)로 고정한 뒤 그 값으로 판정한다 — 13단계 CI에서 배운
        -- 플랫폼별 부동소수 편차를 판정 경계에서 없앤다(맥/러너 동일 결과).
        round(c.after_avg_ms / nullif(c.before_avg_ms, 0), 2)                    as latency_ratio
    from compared c

)

select
    j.instance_id,
    j.query_id,
    j.flip_dt,
    j.prev_plan_hash,
    j.new_plan_hash,
    round(j.before_avg_ms, 2)                                                    as before_avg_ms,
    round(j.after_avg_ms, 2)                                                     as after_avg_ms,
    j.latency_ratio,
    j.before_days,
    j.after_days,
    coalesce(j.after_calls, 0)                                                   as after_calls,
    -- 판정. 지어내지 않는 정직 상태를 먼저 거른다: 관측 부족(PENDING)·창 오염(AMBIGUOUS).
    case
        when j.before_days = 0 or j.after_days = 0 then 'PENDING'
        when j.window_contaminated                 then 'AMBIGUOUS'
        when coalesce(j.after_calls, 0)
             < coalesce(th.min_after_calls, {{ default_min_calls }})  then 'NEUTRAL'
        when j.latency_ratio
             >= coalesce(th.regress_ratio, {{ default_ratio }})       then 'REGRESSED'
        when j.latency_ratio
             <= round(1.0 / coalesce(th.regress_ratio, {{ default_ratio }}), 2) then 'IMPROVED'
        else 'NEUTRAL'
    end                                                                          as verdict,
    current_timestamp                                                            as computed_at
from judged j
left join {{ ref('plan_regression_thresholds') }} th
    on j.instance_id = th.instance_id
