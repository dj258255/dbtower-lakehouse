-- 누적 스냅샷 정규화(staging).
--
-- 원천 raw는 그대로 두면 두 가지 이유로 델타 계산을 못 한다.
--   1) (instance_id, query_id, captured_at) 중복: 같은 지문(query_id)에 둘 이상의
--      누적 계열이 얽혀 있다(핑거프린트 충돌 — 예: "SHOW REPLICA STATUS"가
--      calls=302 계열과 calls=55 계열로 동시 존재). 시간순으로 늘어놓으면
--      302, 55, 302, 56 ... 처럼 감소가 섞여 가짜 리셋으로 보인다.
--   2) 이 얽힘을 풀지 않으면 lag() 차분이 오염된다.
--
-- 해법: (instance_id, query_id, dt, captured_at) 단위로 누적값을 SUM 한다.
-- 단조 비감소 계열들의 합도 단조 비감소이므로, 지문 단위 '총 활동'의 누적 계열이
-- 깔끔하게 복원된다. 델타 계산은 marts(fct_query_daily)에서 이 위에 얹는다.
with raw as (

    select
        instance_id,
        query_id,
        dt,
        captured_at,
        calls,
        total_time_ms,
        rows_examined,
        query_text
    from {{ source('raw', 'query_snapshot') }}

)

select
    instance_id,
    query_id,
    cast(dt as date)         as dt,
    captured_at,
    sum(calls)               as calls,
    sum(total_time_ms)       as total_time_ms,
    sum(rows_examined)       as rows_examined,
    max(query_text)          as query_text
from raw
group by
    instance_id,
    query_id,
    cast(dt as date),
    captured_at
