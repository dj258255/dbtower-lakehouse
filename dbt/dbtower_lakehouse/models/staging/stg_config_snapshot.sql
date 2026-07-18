-- 설정 스냅샷 스테이징 (18단계) — 타입 정리 + dt 캐스팅. 매 수집 1행(무변경도).
-- change_count·baseline은 그대로 흘려보낸다(마트가 "변경 있던 날"을 세는 재료).
{{ config(materialized="view") }}
select
    id,
    instance_id,
    captured_at,
    cast(dt as date)   as dt,
    param_hash,
    change_count,
    baseline
from {{ source('raw', 'config_snapshot') }}
