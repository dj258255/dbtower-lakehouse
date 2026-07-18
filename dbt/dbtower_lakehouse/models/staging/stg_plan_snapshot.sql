-- 플랜 변경 스테이징.
{{ config(materialized="view") }}
select id, instance_id, query_id, plan_hash, captured_at, cast(dt as date) as dt
from {{ source('raw', 'plan_snapshot') }}
