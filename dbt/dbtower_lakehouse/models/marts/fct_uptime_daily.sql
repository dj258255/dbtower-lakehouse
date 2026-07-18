-- 일별 가용성 팩트 (21단계) — "이 인스턴스, 이 날 몇 % 떠 있었나".
--
-- 1분 폴링 샘플을 하루로 접는다. uptime_pct = up 샘플 / 전체 샘플. ping은 up일 때만 의미가
-- 있어(다운 시 ping은 타임아웃/0) up=true 샘플로만 평균·p95를 낸다. 증분 delete+insert(dt 워터마크).
{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["instance_id", "dt"],
        on_schema_change="fail",
    )
}}
with samples as (
    select * from {{ ref('stg_health_sample') }}
    {% if is_incremental() %}
    {% set max_dt = run_query("select max(dt) from " ~ this).columns[0].values()[0] %}
    where dt >= '{{ max_dt }}'
    {% endif %}
)
select
    instance_id,
    dt,
    count(*)                                                    as samples,
    count(*) filter (up)                                        as up_samples,
    count(*) filter (not up)                                    as down_samples,
    round(100.0 * count(*) filter (up) / nullif(count(*), 0), 2) as uptime_pct,
    round(avg(ping_millis) filter (up), 1)                      as avg_ping_ms,
    round(quantile_cont(ping_millis, 0.95) filter (up), 1)      as p95_ping_ms
from samples
group by instance_id, dt
