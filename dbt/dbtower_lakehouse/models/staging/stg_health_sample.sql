-- 가용성 헬스 샘플 스테이징 (21단계) — 타입·dt 캐스팅. up 여부·ping_millis를 그대로.
{{ config(materialized="view") }}
select
    id,
    instance_id,
    sampled_at,
    cast(dt as date)   as dt,
    up,
    ping_millis
from {{ source('raw', 'health_sample') }}
