-- 대기 이벤트 스테이징 — 타입 정리만(의미 해석은 fct·소비자 몫).
{{ config(materialized="view") }}
select id, instance_id, captured_at, cast(dt as date) as dt,
       event_name, category, wait_count, total_ms
from {{ source('raw', 'wait_event_snapshot') }}
