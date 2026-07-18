-- 최근 30일 인스턴스별 상위 대기 (14단계 소비층) — "요즘 이 DB는 주로 뭘 기다리나".
--
-- delta_ms(누적 기종의 발생량) 기준 랭킹 — 스냅샷/큐 기종(PG·Mongo)은 delta가 0에
-- 가까울 수 있어 last_ms 합계도 함께 실어 정직하게 구분한다(기종 축이 없는 것이 이
-- 창고의 한계 — 소비자는 DBTower 인스턴스 화면에서 기종을 안다).
{{ config(materialized="table") }}
with windowed as (
    select * from {{ ref('fct_wait_event_daily') }}
    where dt >= (select max(dt) from {{ ref('fct_wait_event_daily') }})
                - interval {{ var('wait_window_days', 30) }} day
),
ranked as (
    select
        instance_id, event_name,
        any_value(category)  as category,
        sum(delta_ms)        as total_delta_ms,
        sum(delta_count)     as total_delta_count,
        sum(last_ms)         as sum_last_ms,
        count(distinct dt)   as days_seen,
        row_number() over (partition by instance_id order by sum(delta_ms) desc) as rank_in_instance
    from windowed
    group by instance_id, event_name
)
select instance_id, rank_in_instance, event_name, category,
       total_delta_ms, total_delta_count, sum_last_ms, days_seen,
       current_timestamp as computed_at
from ranked
where rank_in_instance <= {{ var('wait_top_k', 10) }}
