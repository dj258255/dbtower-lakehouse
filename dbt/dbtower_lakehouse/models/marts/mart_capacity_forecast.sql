-- 용량 예측 마트 (Phase 13 C3·C4) — "이 인스턴스 몇 달 뒤 임계에 닿나".
--
-- 지평 경계(13단계): 단기(시간~일) 디스크 ETA는 DBTower(78절, Prometheus 라이브)의 몫이고,
-- 이 마트는 장기(주~분기) — 일별 크기 시계열의 선형 추세다. 상용(Redgate·SolarWinds·
-- Oracle Ops Insights)도 선형 외삽이 기본이라 regr_slope(최소자승)로 충분하다(ML 과잉 금지).
--
-- 발화 주체(13단계 확정): 이 마트는 판정 컬럼까지만 계산한다 — 알림을 직접 쏘지 않는다.
-- 소비는 Metabase(사람, pull) 또는 DBTower reverse ETL(기계, push)의 몫.
--
-- 임계(분모)의 원천 ①: seeds/capacity_thresholds.csv(사용자 설정). kind가 알림의 의미를
-- 정한다(물리=포화 D-day / autoscale_max=상한 도달 / budget·none=증가율만). 원천 ②(기종이
-- 아는 볼륨)는 fct의 volume_*가 채워지면 threshold 부재 시 폴백으로 쓸 수 있다(현재 NULL).
{{ config(materialized="table") }}
with daily as (

    -- 인스턴스 수준 집계 — 오브젝트 합(top-N 수집이라 "관측된 오브젝트 합"임을 정직 표기).
    select
        instance_id,
        dt,
        sum(total_bytes) as total_bytes
    from {{ ref('fct_size_daily') }}
    group by instance_id, dt

),

windowed as (

    -- 최근 N일 창(기본 30) — 오래된 추세가 최근 변화를 희석하지 않게.
    select *
    from daily
    where dt >= (select max(dt) from daily) - interval {{ var('capacity_window_days', 30) }} day

),

volume_limit as (

    -- 임계 원천 ②(기종이 아는 볼륨/상한 — MSSQL 볼륨 총량·Oracle autoextend 상한).
    -- 최신 dt의 값만 쓴다. 없으면 NULL — seed(원천 ①)가 우선하고, 둘 다 없으면 증가율만.
    select instance_id, max(coalesce(max_bytes, volume_total_bytes)) as volume_threshold_bytes
    from {{ ref('fct_size_daily') }}
    where dt = (select max(dt) from {{ ref('fct_size_daily') }})
    group by instance_id

),

trend as (

    select
        instance_id,
        count(*)                                          as days_observed,
        max(dt)                                           as latest_dt,
        max_by(total_bytes, dt)                           as current_bytes,
        -- 최소자승 기울기: bytes/일. epoch_day는 dt 기반(타임존 무관 — 13단계 함정 노트).
        regr_slope(total_bytes, date_diff('day', DATE '1970-01-01', dt)) as slope_bytes_per_day,
        regr_r2(total_bytes, date_diff('day', DATE '1970-01-01', dt))    as trend_r2
    from windowed
    group by instance_id

)

select
    t.instance_id,
    t.latest_dt,
    t.days_observed,
    -- 관측 부족이면 추세를 지어내지 않는다 — D1 베이스라인·D6 마트와 같은 "학습 중" 정직 패턴.
    t.days_observed < {{ var('capacity_min_days', 14) }}          as learning,
    t.current_bytes,
    round(t.slope_bytes_per_day, 2)                                as slope_bytes_per_day,
    round(t.trend_r2, 4)                                           as trend_r2,
    coalesce(th.threshold_bytes, vl.volume_threshold_bytes)        as threshold_bytes,
    case
        when th.threshold_bytes is not null then coalesce(th.threshold_kind, 'none')
        when vl.volume_threshold_bytes is not null then 'volume_reported'
        else 'none'
    end                                                            as threshold_kind,
    -- 잔여일 = (임계 − 현재) / 기울기. 성장 없음·역성장·임계 없음·학습 중이면 NULL(지어내지 않음).
    case
        when t.days_observed >= {{ var('capacity_min_days', 14) }}
             and coalesce(th.threshold_bytes, vl.volume_threshold_bytes) is not null
             and t.slope_bytes_per_day > 0
             and coalesce(th.threshold_bytes, vl.volume_threshold_bytes) > t.current_bytes
        then floor((coalesce(th.threshold_bytes, vl.volume_threshold_bytes) - t.current_bytes) / t.slope_bytes_per_day)
    end                                                            as days_to_threshold,
    -- 판정 컬럼(발화는 안 함): kind가 의미를 정한다 — 13단계 "알림의 의미" 표 그대로.
    case
        when t.days_observed < {{ var('capacity_min_days', 14) }} then 'learning'
        when t.slope_bytes_per_day <= 0 then 'stable_or_shrinking'
        when coalesce(th.threshold_bytes, vl.volume_threshold_bytes) is null then 'growth_only'
        when (coalesce(th.threshold_bytes, vl.volume_threshold_bytes) - t.current_bytes) / t.slope_bytes_per_day <= 30 then 'd30'
        when (coalesce(th.threshold_bytes, vl.volume_threshold_bytes) - t.current_bytes) / t.slope_bytes_per_day <= 90 then 'd90'
        else 'ok'
    end                                                            as risk_flag,
    current_timestamp                                              as computed_at
from trend t
left join {{ ref('capacity_thresholds') }} th
    on t.instance_id = th.instance_id
left join volume_limit vl
    on t.instance_id = vl.instance_id
