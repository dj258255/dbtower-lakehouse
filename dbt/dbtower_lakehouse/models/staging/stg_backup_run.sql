-- 백업 이력 스테이징 — D+1 스냅샷 계약(사후 변이는 재추출이 반영).
{{ config(materialized="view") }}
select id, instance_id, started_at, cast(dt as date) as dt,
       status, backup_type, duration_ms, verify_status, verified_at, remote_location
from {{ source('raw', 'backup_run') }}
