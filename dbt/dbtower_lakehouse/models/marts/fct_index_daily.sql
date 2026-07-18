-- 인덱스 일간 사용 팩트 (17단계).
--
-- scan_count는 통계 리셋(재기동) 이후 누적 카운터다 — query_snapshot의 calls와 같은 성질이라
-- fct_query_daily와 정확히 같은 first-vs-last 델타·클램프를 쓴다. 하루의 실사용은 그날 파티션
-- 양 끝(first/last)의 차분이고, 하루 중 재기동으로 last < first면 GREATEST(0,..)로 클램프한다.
-- 인접 델타 합산이 아니라 양 끝만 보는 이유는 2편 문서와 동일(중간 출렁임에 안 흔들림).
--
-- grain은 (instance_id, table_name, index_name, dt)로 dt 단위 완전 독립 — fct_query_daily와
-- 같은 delete+insert 증분(새 dt만 계산, 당일 재실행은 그 dt만 교체). is_unique·size_bytes는
-- 판정·표기용으로 그날 마지막 관측을 싣는다(인덱스 속성은 하루 안에서 안 바뀐다).
{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["instance_id", "table_name", "index_name", "dt"],
        on_schema_change="fail",
    )
}}
with snap as (

    select
        instance_id,
        table_name,
        index_name,
        dt,
        captured_at,
        scan_count,
        size_bytes,
        is_unique
    from {{ ref('stg_index_usage_snapshot') }}
    {% if is_incremental() %}
    -- fct_query_daily와 같은 리터럴 워터마크(파티션 프루닝). >= 라 최신 dt 재실행도 멱등.
    {% set max_dt = run_query("select max(dt) from " ~ this).columns[0].values()[0] %}
    where dt >= '{{ max_dt }}'
    {% endif %}

),

endpoints as (

    select
        instance_id,
        table_name,
        index_name,
        dt,
        scan_count,
        size_bytes,
        is_unique,
        row_number() over (partition by instance_id, table_name, index_name, dt order by captured_at asc)  as rn_first,
        row_number() over (partition by instance_id, table_name, index_name, dt order by captured_at desc) as rn_last
    from snap

),

diffed as (

    select
        instance_id,
        table_name,
        index_name,
        dt,
        max(case when rn_first = 1 then scan_count end) as first_scans,
        max(case when rn_last  = 1 then scan_count end) as last_scans,
        max(case when rn_last  = 1 then size_bytes end) as size_bytes,
        bool_or(is_unique)                              as is_unique
    from endpoints
    group by instance_id, table_name, index_name, dt

)

select
    instance_id,
    table_name,
    index_name,
    dt,
    greatest(0, last_scans - first_scans) as delta_scans,
    size_bytes,
    is_unique
from diffed
