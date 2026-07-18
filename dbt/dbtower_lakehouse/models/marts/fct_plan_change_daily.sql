-- 일별 플랜 변경 팩트 — "이 쿼리 플랜이 이번 달 몇 번 뒤집혔나"의 재료.
{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["instance_id", "dt"],
        on_schema_change="fail",
    )
}}
with snaps as (
    select * from {{ ref('stg_plan_snapshot') }}
    {% if is_incremental() %}
    {% set max_dt = run_query("select max(dt) from " ~ this).columns[0].values()[0] %}
    where dt >= '{{ max_dt }}'
    {% endif %}
)
select
    instance_id, dt,
    count(*)                 as plan_changes,
    count(distinct query_id) as queries_flipped
from snaps
group by instance_id, dt
