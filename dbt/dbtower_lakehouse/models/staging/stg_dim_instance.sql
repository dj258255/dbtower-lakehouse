-- 인스턴스 차원 스테이징 (20단계) — 타입·dt 캐스팅. 느린 변화 차원이라 여러 dt 스냅샷이 쌓인다.
{{ config(materialized="view") }}
select
    id,
    name,
    type,
    team_label,
    cast(dt as date)   as dt
from {{ source('raw', 'dim_instance') }}
