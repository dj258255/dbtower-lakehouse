-- staging의 그레인은 (instance_id, query_id, dt, captured_at)여야 한다.
-- SUM 집계로 지문 충돌·중복 계열을 접었으므로 중복 키가 남으면 안 된다.
select
    instance_id,
    query_id,
    dt,
    captured_at,
    count(*) as n
from {{ ref('stg_query_snapshot') }}
group by instance_id, query_id, dt, captured_at
having count(*) > 1
