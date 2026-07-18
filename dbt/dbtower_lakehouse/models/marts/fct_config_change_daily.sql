-- 일별 설정 변경 팩트 (18단계) — "이 인스턴스, 이 날 설정이 몇 번 바뀌었나".
--
-- 스파인은 config_snapshot(매 수집 1행 — 무변경도)이라 **"수집됐는데 무변경(quiet)"과 "수집
-- 자체가 없음(gap)"을 구분한다**(백업 공백 마트의 정직함과 같은 결). 변경 상세는
-- config_param_change에서 붙인다. 증분 delete+insert(unique_key=instance_id,dt).
{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["instance_id", "dt"],
        on_schema_change="fail",
    )
}}

{% if is_incremental() %}
{% set max_dt = run_query("select max(dt) from " ~ this).columns[0].values()[0] %}
{% endif %}

with snaps as (
    select instance_id, dt from {{ ref('stg_config_snapshot') }}
    {% if is_incremental() %}where dt >= '{{ max_dt }}'{% endif %}
),
events as (
    select instance_id, dt, param_name from {{ ref('stg_config_param_change') }}
    {% if is_incremental() %}where dt >= '{{ max_dt }}'{% endif %}
),
sn as (
    select instance_id, dt, count(*) as cycles_collected
    from snaps group by instance_id, dt
),
ev as (
    select instance_id, dt,
           count(*)                   as change_events,
           count(distinct param_name) as params_changed
    from events group by instance_id, dt
)
select
    sn.instance_id,
    sn.dt,
    sn.cycles_collected,
    coalesce(ev.change_events, 0)   as change_events,
    coalesce(ev.params_changed, 0)  as params_changed
from sn
left join ev on sn.instance_id = ev.instance_id and sn.dt = ev.dt
