select
    customer_id,
    customer_name,
    region,
    total_orders,
    total_spend,
    pending_orders,
    cancelled_orders,
    last_order_date,
    alert_reason,
    alert_priority
from {{ source('spark_output', 'int_customer_alerts_parquet') }}