-- 미사용 인덱스 장기 판정 (17단계) — "이 인덱스 지워도 되나"를 분기 창으로 답한다.
--
-- 이 저장소만 할 수 있는 판정이다: 7일 관측은 "지난주 재기동 이후 0회"와 "분기 내내 0회"를
-- 구분 못 한다. fct_index_daily의 일간 실사용(델타)을 창(기본 90일)으로 합산해, 관측 기간이
-- 충분한데도 사용이 0인 비유니크 인덱스만 삭제 후보로 올린다.
--
-- 판정 예외(성급한 삭제 방지):
--   (1) 유니크/PK 뒷받침 인덱스는 미사용이라도 제약 유지에 필요 → constraint_backed로 제외.
--       (FK 뒷받침은 원천 스냅샷이 is_unique만 주고 FK 여부를 안 담아 판정 불가 — note에 정직 표기.)
--   (2) 관측 기간이 창 하한 미만이면 insufficient_observation — 0회를 미사용으로 단정하지 않는다.
--   (3) 레플리카 전용 사용은 프라이머리 통계만 수집해 오판 가능 — note에 한계 명시.
-- 창 앵커는 데이터의 최신 dt(current_date 아님) — 라이브·픽스처 양쪽에서 재현되게.
{{ config(materialized="table") }}

{% set window_days = var('index_verdict_window_days', 90) %}
{% set min_obs_days = var('index_verdict_min_obs_days', 60) %}

with daily as (

    select * from {{ ref('fct_index_daily') }}

),

anchor as (

    select max(dt) as as_of from daily

),

windowed as (

    -- DuckDB: DATE - INTEGER 는 N일 뺀 날짜. 앵커에서 창 길이만큼 거슬러 올라간 이후만.
    select d.*
    from daily d, anchor a
    where d.dt > a.as_of - {{ window_days }}

),

agg as (

    select
        instance_id,
        table_name,
        index_name,
        sum(delta_scans)                                as total_scans,
        max(case when delta_scans > 0 then dt end)      as last_used_dt,
        date_diff('day', min(dt), max(dt))              as observation_days,
        bool_or(is_unique)                              as is_unique,
        max(size_bytes)                                 as size_bytes
    from windowed
    group by instance_id, table_name, index_name

)

select
    instance_id,
    table_name,
    index_name,
    total_scans,
    last_used_dt,
    observation_days,
    is_unique,
    size_bytes,
    -- 우선순위: 사용 중이면(스캔>0) 관측 길이와 무관하게 in_use. 그 다음 유니크(미사용이라도 제약).
    -- 그 다음 관측 부족(판정 보류). 남는 것만 삭제 후보(비유니크·충분 관측·사용 0).
    case
        when total_scans > 0                  then 'in_use'
        when is_unique                        then 'constraint_backed'
        when observation_days < {{ min_obs_days }} then 'insufficient_observation'
        else 'candidate_unused'
    end                                                 as verdict,
    -- 조언 어휘(삭제 지시 아님) + 정직한 한계. FK 뒷받침·레플리카 전용 사용은 이 데이터로 판정 불가.
    case
        when total_scans > 0
            then '창 내 사용 ' || total_scans || '회 — 사용 중.'
        when is_unique
            then '유니크/PK 뒷받침 — 미사용이라도 제약 유지에 필요할 수 있어 제외.'
        when observation_days < {{ min_obs_days }}
            then '관측 ' || observation_days || '일(창 하한 ' || {{ min_obs_days }} || '일 미만) — 판정 보류. 더 쌓인 뒤 재판정.'
        else '창(' || {{ window_days }} || '일) 내 사용 0회 — 삭제 검토 후보. 단 FK 뒷받침·레플리카 전용 사용은 이 통계로 알 수 없으니 확인 후 사람이 실행.'
    end                                                 as note
from agg
