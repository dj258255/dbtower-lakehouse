-- 변경 이벤트 통합 스트림 (19단계) — "무엇을 바꿨든" 한 형태로 모은다.
--
-- mart_config_impact가 이걸 성능 신호(플랜 뒤집힘·지연)와 겹쳐 "변경 뒤 회귀"를 판정한다.
-- 원인의 축이 여럿이라(파라미터 변경 / 스키마 변경 / …), 상관을 특정 소스에 묶지 않고
-- 이 통합 스트림 위에서 계산한다 — 소스가 늘어도 상관 로직은 그대로다.
--
-- 지금은 설정 변경(config)만. **스키마 변경(change_review, DBTower V28)은 자리를 열어둔다** —
-- 필요해지면 아래 union all에 소스 하나(approved DDL의 승인일·대상)를 추가하면 상관이 자동으로
-- 그 축까지 커버한다. change_review는 저빈도 사후변이(승인은 나중에 UPDATE)라 편입 시 backup_run
-- 식 D+1 계약이 필요하다(그때 tables.py 레지스트리 + stg 추가). 지금은 짊어지지 않는다.
{{ config(materialized="view") }}
select
    instance_id,
    dt              as change_dt,
    'config'        as change_source,   -- 변경의 종류(config | 향후 schema)
    param_name      as change_key,       -- 무엇이(파라미터명 | 향후 대상 오브젝트)
    change_kind,                         -- CHANGED/ADDED/REMOVED | 향후 승인 상태
    old_value,
    new_value
from {{ ref('stg_config_param_change') }}

-- 미래(스키마 변경, 자리만): stg_change_review(승인된 DDL)를 아래 형태로 union all 하면 된다.
--   union all
--   select instance_id, decided_at::date as change_dt, 'schema' as change_source,
--          'ddl' as change_key, status as change_kind, null as old_value, target_sql_masked as new_value
--   from <stg_change_review> where status = 'APPROVED'
-- (dbt는 주석 안의 ref() 문법도 의존으로 파싱하므로 여기선 일부러 ref를 안 쓴다.)
