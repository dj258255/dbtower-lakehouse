-- 일별 백업 이력 팩트 — "분기 백업 실패율 추세"의 재료. D+1 재추출이 사후 변이(verify)를 반영.
{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["instance_id", "dt"],
        on_schema_change="fail",
    )
}}
with runs as (
    select * from {{ ref('stg_backup_run') }}
    {% if is_incremental() %}
    {% set max_dt = run_query("select max(dt) from " ~ this).columns[0].values()[0] %}
    where dt >= '{{ max_dt }}'
    {% endif %}
)
select
    instance_id, dt,
    count(*)                                                   as total_runs,
    count(*) filter (status = 'SUCCESS')                       as success_runs,
    count(*) filter (status = 'FAILED')                        as failed_runs,
    count(*) filter (backup_type = 'LOG')                      as log_runs,
    count(*) filter (verify_status = 'VERIFIED')               as verified_runs,
    count(*) filter (remote_location is not null)              as offsite_runs,
    max(started_at)                                            as last_started_at
from runs
group by instance_id, dt
