-- 인덱스 사용 통계 스테이징 (17단계) — 타입 정리 + dt 캐스팅만. scan_count는 누적 카운터라
-- 델타 변환은 fct_index_daily에서(query_snapshot과 같은 first-vs-last 패턴). is_unique·size_bytes는
-- 판정·표기용으로 그대로 흘려보낸다.
{{ config(materialized="view") }}
select
    id,
    instance_id,
    captured_at,
    cast(dt as date)   as dt,
    table_name,
    index_name,
    scan_count,
    size_bytes,
    is_unique
from {{ source('raw', 'index_usage_snapshot') }}
