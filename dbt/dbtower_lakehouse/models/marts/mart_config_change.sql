-- 설정 변경 타임라인 마트 (18단계) — "언제 무엇이 어떻게 바뀌었나"의 서빙.
--
-- DBTower는 이 이벤트를 7일류로 지운다(자체 retention sweep). 여기서 장기로 보관해
-- "3개월 전 언제부터 이 인스턴스 work_mem이 달라졌나"를 되짚게 한다. 최근 N일(기본 90) 창.
--
-- 발화는 안 한다(13단계 원칙): 타임라인까지만 계산하고, 급변 알림은 DBTower/Metabase의 몫.
-- 정직 한계: "누가" 바꿨는지는 대상 DB가 안 줘서 없다 — old/new_value와 언제까지만.
--
-- 컬럼을 명시(select * 회피)하고 창은 anchor CTE로 잡는다 — hive 파티션 뷰 위의 select *를
-- 자기참조 max 서브쿼리와 겹치면 DuckDB 바인더가 내부 오류를 낸다(instance_id·dt가 파일과
-- 파티션 경로에 중복). mart_query_regression과 같은 date - 정수 산수로 안정화.
{{ config(materialized="table") }}

with events as (

    select
        instance_id,
        captured_at,
        dt,
        param_name,
        old_value,
        new_value,
        change_kind
    from {{ ref('stg_config_param_change') }}

),

anchor as (

    select max(dt) as max_dt from events

)

select
    e.instance_id,
    e.captured_at,
    e.dt,
    e.param_name,
    e.old_value,
    e.new_value,
    e.change_kind,
    current_timestamp as computed_at
from events e
cross join anchor a
where e.dt >= a.max_dt - {{ var('config_window_days', 90) }}
order by e.captured_at desc
