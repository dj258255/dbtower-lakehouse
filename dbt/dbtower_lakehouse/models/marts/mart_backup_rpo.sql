-- 백업 공백(RPO) 판정 마트 (16단계 G4) — "이 인스턴스, 마지막 성공 백업이 며칠 전인가".
--
-- DBA 최악의 시나리오: 복구하려고 보니 백업이 몇 주째 조용히 안 돌고 있었다. 실패는
-- 시끄럽지만(FAILED 행이 남음) **공백은 조용하다**(행 자체가 없음) — fct_backup_daily는
-- 일별 성공/실패 집계까지고, "마지막 성공 이후 경과일" 판정이 없었다. 이 마트가 그 침묵을
-- 판정 컬럼으로 만든다.
--
-- 두 가지 함정을 피한다:
--  1) 기준 시각을 벽시계(오늘)로 잡으면 파이프라인 중단과 백업 중단을 구분 못 한다(파이프라인이
--     죽어도 gap이 자란다). 기준은 **창고 전체 max dt** — 파이프라인 신선도는 게이트·deadman의
--     관심사라 여기서 섞지 않는다(관심사 분리).
--  2) 인스턴스 유니버스를 백업 테이블에서 뽑으면 "백업 기록이 아예 없는" 인스턴스가 사라진다.
--     fct_query_daily(전 기종 공통 관측)에서 유니버스를 잡고 left join해, 기록 부재도 행으로
--     드러나게 한다 — database_instance 신규 추출 없이 전수 확보(원천 계약 불변).
--
-- 정직한 한계: "성공 백업 관측 없음"(no_backup_observed)은 두 경우가 섞여 있다 — (i) 백업이
-- 안 돎, (ii) 그 기종의 백업 이력을 DBTower가 아직 수집 안 함. 이 창고엔 기종 축이 없어(mart_wait_top
-- 과 같은 한계) 둘을 구분 못 한다. 그래서 breach라 단정하지 않고 사실(no_backup_observed)만
-- 싣는다 — 소비자(DBTower 인스턴스 화면)가 기종을 알고 해석한다.
{{ config(materialized="table") }}

with universe as (

    -- 감시 대상 인스턴스 = query 팩트에 나타난 전부(전 기종 공통 원천).
    select distinct instance_id from {{ ref('fct_query_daily') }}

),

anchor as (

    -- 기준일 = 창고 최신 dt(벽시계 아님).
    select max(dt) as as_of_dt from {{ ref('fct_query_daily') }}

),

backup_state as (

    select
        instance_id,
        max(dt) filter (success_runs > 0)   as last_success_dt,
        max(dt) filter (verified_runs > 0)  as last_verified_dt
    from {{ ref('fct_backup_daily') }}
    group by instance_id

)

select
    u.instance_id,
    a.as_of_dt,
    b.last_success_dt,
    b.last_verified_dt,
    -- date_diff로 명시(date-date의 반환 타입 모호성 회피). 성공 백업 없으면 NULL(지어내지 않음).
    case when b.last_success_dt is not null
         then date_diff('day', b.last_success_dt, a.as_of_dt) end               as gap_days,
    (b.last_success_dt is null)                                                  as no_successful_backup,
    coalesce(th.max_gap_days, {{ var('backup_max_gap_days', 3) }})               as max_gap_days,
    case
        when b.last_success_dt is null then 'no_backup_observed'
        when date_diff('day', b.last_success_dt, a.as_of_dt)
             > coalesce(th.max_gap_days, {{ var('backup_max_gap_days', 3) }}) then 'breach'
        else 'ok'
    end                                                                          as rpo_status,
    current_timestamp                                                            as computed_at
from universe u
cross join anchor a
left join backup_state b
    on u.instance_id = b.instance_id
left join {{ ref('backup_rpo_thresholds') }} th
    on u.instance_id = th.instance_id
