-- 누적 카운터 특성상 일간 델타는 절대 음수일 수 없다.
-- GREATEST(0, ...) 리셋 클램프가 살아 있으면 이 테스트는 0행을 반환해야 한다.
-- (accepted_range의 min:0 을 dbt_utils 없이 표현한 것.)
select
    instance_id,
    query_id,
    dt,
    delta_calls,
    delta_total_time_ms
from {{ ref('fct_query_daily') }}
where delta_calls < 0
   or delta_total_time_ms < 0
