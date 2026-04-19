-- fct_query_daily의 그레인은 (instance_id, query_id, dt)여야 한다.
select
    instance_id,
    query_id,
    dt,
    count(*) as n
from {{ ref('fct_query_daily') }}
group by instance_id, query_id, dt
having count(*) > 1
