-- 장기 요일×시간대 베이스라인 (Phase 14 D6) — 되쓰기(D7)의 화물.
--
-- DBTower의 이상 감지 베이스라인은 7일 창이라 "매주 월요일 아침 배치 피크" 같은
-- 주간 계절성을 4주 전과 비교하지 못해 오탐한다. 이 마트가 장기 이력(fct_query_hourly)
-- 으로 (instance, query, dow, hour)별 delta_calls의 평균/표준편차를 계산하고,
-- D7이 이것을 원천 별도 테이블(baseline_longterm)로 되쓴다 — DBTower BaselineService가
-- 관측 충분 시 가중 병합(D8, DBTower 몫).
--
-- 지표는 시간당 delta_calls(호출량) — DBTower D1 이상감지가 보는 부하 축과 정렬.
-- dow는 UTC 기준(원천이 UTC 고정 — 계산은 UTC, 표시만 로컬이 이 패밀리의 규약).
-- DuckDB dayofweek: 일요일=0 .. 토요일=6.
--
-- 두 가드(14단계 명세):
-- - min_observations: 관측이 적은 (dow,hour) 버킷은 통계가 아니라 소음이다 —
--   DBTower BaselineService의 8관측 게이트와 정렬(기본 8, var로 조정).
-- - top-K: instance×query×168버킷은 카디널리티 폭발 경로다 — 인스턴스별 호출량
--   상위 K 쿼리만(기본 500). 잘린 쿼리는 "장기 없음 → DBTower 현행 7일 창 폴백"이
--   계약이지 오류가 아니다.
{{ config(materialized="table") }}
with hourly as (

    select
        instance_id,
        query_id,
        dayofweek(cast(dt as date))::smallint as dow,
        hour,
        delta_calls
    from {{ ref('fct_query_hourly') }}

),

top_queries as (

    -- 인스턴스별 총 호출량 상위 K — 카디널리티 폭발 가드.
    select instance_id, query_id
    from (
        select
            instance_id,
            query_id,
            sum(delta_calls) as total_calls,
            row_number() over (
                partition by instance_id order by sum(delta_calls) desc
            ) as rank_in_instance
        from hourly
        group by instance_id, query_id
    )
    where rank_in_instance <= {{ var('baseline_top_k_queries', 500) }}

),

stats as (

    select
        h.instance_id,
        h.query_id,
        h.dow,
        h.hour,
        avg(h.delta_calls)         as mean_delta_calls,
        stddev_samp(h.delta_calls) as stddev_delta_calls,
        count(*)                   as observations
    from hourly h
    join top_queries t
        on h.instance_id = t.instance_id and h.query_id = t.query_id
    group by h.instance_id, h.query_id, h.dow, h.hour

)

select
    instance_id,
    query_id,
    dow,
    hour,
    mean_delta_calls,
    -- 관측 1개면 stddev_samp가 NULL — 병합 쪽(D8)이 NULL을 "분산 정보 없음"으로 읽는다.
    stddev_delta_calls,
    observations,
    current_timestamp as computed_at
from stats
where observations >= {{ var('baseline_min_observations', 8) }}
