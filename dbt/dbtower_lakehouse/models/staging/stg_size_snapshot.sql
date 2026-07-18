-- 크기 스냅샷 스테이징 (Phase 13) — 타입 정리만. 크기는 절대값(누적 카운터 아님)이라
-- 델타 변환이 필요 없다 — 하루의 대푯값은 fct에서 "그날 마지막 관측"으로 고른다.
{{ config(materialized="view") }}
select
    id,
    instance_id,
    captured_at,
    cast(dt as date)       as dt,
    object_type,
    object_name,
    row_estimate,
    data_bytes,
    index_bytes,
    data_bytes + index_bytes as total_bytes,
    volume_total_bytes,
    volume_available_bytes,
    max_bytes
from {{ source('raw', 'size_snapshot') }}
