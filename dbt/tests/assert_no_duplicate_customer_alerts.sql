-- This test fails if any customer_id + alert appears more than once in the final target list
select
    customer_id,
    alert_reason,
    count(*) as record_count
from {{ ref('fct_customer_targets') }}
group by customer_id, alert_reason
having count(*) > 1