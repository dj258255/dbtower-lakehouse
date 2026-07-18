-- 최근 30일 인스턴스별 상위 대기 (14단계 소비층) — "요즘 이 DB는 주로 뭘 기다리나".
--
-- delta_ms(누적 기종의 발생량) 기준 랭킹 — 스냅샷/큐 기종(PG·Mongo)은 delta가 0에
-- 가까울 수 있어 last_ms 합계도 함께 실어 정직하게 구분한다. 20단계부터 기종(engine)을
-- dim_instance에서 조인해 실어, 소비자가 delta/last의 의미(누적 vs 스냅샷 vs 큐)를
-- 창고 안에서 바로 해석한다 — "기종은 DBTower 화면에서 보라"던 각주가 사라진다.
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
select r.instance_id, di.instance_name, di.engine,
       r.rank_in_instance, r.event_name, r.category,
       r.total_delta_ms, r.total_delta_count, r.sum_last_ms, r.days_seen,
       current_timestamp as computed_at
from ranked r
left join {{ ref('dim_instance') }} di on r.instance_id = di.instance_id
where r.rank_in_instance <= {{ var('wait_top_k', 10) }}
