-- 일별 대기 이벤트 팩트 (14단계 소비층) — "그 날 뭘 기다렸나"의 재료.
--
-- 기종별 의미 차이(CONTRACT §1-1)를 한 산식으로 뭉개지 않는다: 누적 기종(MySQL/MSSQL/
-- Oracle)은 양 끝 차분(delta_*)이 그날 발생량이고, 스냅샷 기종(PG)·큐 기종(Mongo)은
-- last_*가 그날의 마지막 상태다. 팩트는 둘 다 실어 소비자(마트·대시보드)가 기종에 맞게
-- 고른다 — 어느 쪽이 맞는지 지어내지 않는다.
{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["instance_id", "event_name", "dt"],
        on_schema_change="fail",
    )
}}
with snap as (
    select * from {{ ref('stg_wait_event') }}
    {% if is_incremental() %}
    {% set max_dt = run_query("select max(dt) from " ~ this).columns[0].values()[0] %}
    where dt >= '{{ max_dt }}'
    {% endif %}
),
endpoints as (
    select *,
        row_number() over (partition by instance_id, event_name, dt order by captured_at asc)  as rn_first,
        row_number() over (partition by instance_id, event_name, dt order by captured_at desc) as rn_last
    from snap
),
diffed as (
    select
        instance_id, event_name, dt,
        any_value(category)                                   as category,
        max(case when rn_first = 1 then wait_count end)       as first_count,
        max(case when rn_last  = 1 then wait_count end)       as last_count,
        max(case when rn_first = 1 then total_ms end)         as first_ms,
        max(case when rn_last  = 1 then total_ms end)         as last_ms,
        count(distinct captured_at)                           as observations
    from endpoints
    group by instance_id, event_name, dt
)
select
    instance_id, event_name, category, dt, observations,
    greatest(0, last_count - first_count) as delta_count,
    greatest(0, last_ms - first_ms)       as delta_ms,
    last_count, last_ms
from diffed
