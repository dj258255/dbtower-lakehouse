-- 일별 오브젝트 크기 팩트 (Phase 13 C3) — 용량 추세의 재료.
--
-- 크기는 절대값이라 하루의 대푯값은 "그날 마지막 관측"이다(6시간 주기 중 최신).
-- 증분 전략은 검증된 fct_query_daily 패턴 복제 — delete+insert + 컴파일 타임 워터마크
-- 리터럴(스칼라 서브쿼리는 hive 파티션 프루닝이 안 걸린다 — Phase 10 실측).
{{
    config(
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["instance_id", "object_type", "object_name", "dt"],
        on_schema_change="fail",
    )
}}
with snap as (

    select *
    from {{ ref('stg_size_snapshot') }}
    {% if is_incremental() %}
    {% set max_dt = run_query("select max(dt) from " ~ this).columns[0].values()[0] %}
    where dt >= '{{ max_dt }}'
    {% endif %}

),

latest as (

    select
        *,
        row_number() over (
            partition by instance_id, object_type, object_name, dt
            order by captured_at desc, id desc
        ) as rn
    from snap

)

select
    instance_id,
    object_type,
    object_name,
    dt,
    row_estimate,
    data_bytes,
    index_bytes,
    total_bytes,
    volume_total_bytes,
    volume_available_bytes,
    max_bytes
from latest
where rn = 1
