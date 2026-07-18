-- 인스턴스 차원 (20단계) — 기종 축. "기종은 DBTower 화면에서 보라"던 마트들의 각주를 창고
-- 안에서 걷어내는 조인 대상. database_instance는 이미 매 오프로드가 읽는 원천(instance id 때문)
-- 이라 컬럼 두 개(name·type)를 더 실은 것뿐.
--
-- 느린 변화 차원: 여러 dt 스냅샷 중 **최신 dt의 상태**만 취해 현재 인스턴스 목록을 만든다.
-- (기종·이름 변화 이력이 필요하면 stg_dim_instance를 직접 보면 된다 — 여기선 현재 상태.)
{{ config(materialized="table") }}
with ranked as (
    select
        id,
        name,
        type,
        team_label,
        row_number() over (partition by id order by dt desc) as rn
    from {{ ref('stg_dim_instance') }}
)
select
    id            as instance_id,
    name          as instance_name,
    type          as engine,          -- MYSQL | POSTGRESQL | MSSQL | MONGODB | ORACLE
    team_label
from ranked
where rn = 1
