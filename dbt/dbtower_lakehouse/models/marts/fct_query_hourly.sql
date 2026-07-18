-- 시간대별 발생량 팩트 (Phase 14 D5).
--
-- 왜 필요한가: 장기 베이스라인(D6)은 요일×시간대(dow×hour) 통계를 요구하는데,
-- 일간 팩트(fct_query_daily)에는 시간 축이 접혀 있어 hour별 통계가 불가능하다.
-- staging에서 시간대별 델타를 직접 뽑는 팩트를 신설한다.
--
-- 원리는 fct_query_daily와 동일한 양 끝 차분 — 단 창이 (dt, hour)다. 같은 시간대
-- 안의 first/last 스냅샷 차분이 그 시간의 발생량이다. 한 시간에 스냅샷이 1개뿐이면
-- 델타 0(일간 팩트의 "하루 1스냅샷 델타 0"과 같은 정직한 한계 — 과대계상보다 낫다).
-- 리셋 클램프(GREATEST 0)도 동일하게 승계한다.
--
-- 증분 전략은 fct_query_daily의 검증된 패턴 복제 — delete+insert + 컴파일 타임
-- 워터마크 리터럴(스칼라 서브쿼리는 파티션 프루닝이 안 걸린다 — Phase 10 실측).
{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["instance_id", "query_id", "dt", "hour"],
        on_schema_change="fail",
    )
}}
with snap as (

    select
        instance_id,
        query_id,
        dt,
        extract(hour from captured_at)::smallint as hour,
        captured_at,
        calls,
        total_time_ms
    from {{ ref('stg_query_snapshot') }}
    {% if is_incremental() %}
    {% set max_dt = run_query("select max(dt) from " ~ this).columns[0].values()[0] %}
    where dt >= '{{ max_dt }}'
    {% endif %}

),

endpoints as (

    select
        instance_id,
        query_id,
        dt,
        hour,
        calls,
        total_time_ms,
        row_number() over (
            partition by instance_id, query_id, dt, hour order by captured_at asc
        ) as rn_first,
        row_number() over (
            partition by instance_id, query_id, dt, hour order by captured_at desc
        ) as rn_last
    from snap

),

diffed as (

    select
        instance_id,
        query_id,
        dt,
        hour,
        max(case when rn_first = 1 then calls end)         as first_calls,
        max(case when rn_last  = 1 then calls end)         as last_calls,
        max(case when rn_first = 1 then total_time_ms end) as first_time_ms,
        max(case when rn_last  = 1 then total_time_ms end) as last_time_ms
    from endpoints
    group by instance_id, query_id, dt, hour

)

select
    instance_id,
    query_id,
    dt,
    hour,
    greatest(0, last_calls - first_calls)                  as delta_calls,
    greatest(0, last_time_ms - first_time_ms)              as delta_total_time_ms,
    greatest(0, last_time_ms - first_time_ms)
        / nullif(greatest(0, last_calls - first_calls), 0) as avg_latency_ms
from diffed
