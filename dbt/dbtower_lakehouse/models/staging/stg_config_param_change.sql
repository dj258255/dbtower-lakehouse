-- 설정 변경 이벤트 스테이징 (18단계) — 바뀐 파라미터만(append-only). 타입 정리 + dt 캐스팅.
-- old/new_value는 마스킹된 표기 그대로(원천이 이미 민감값 마스킹). "누가"는 원천에 없다.
{{ config(materialized="view") }}
select
    id,
    snapshot_id,
    instance_id,
    captured_at,
    cast(dt as date)   as dt,
    param_name,
    old_value,
    new_value,
    change_kind
from {{ source('raw', 'config_param_change') }}
