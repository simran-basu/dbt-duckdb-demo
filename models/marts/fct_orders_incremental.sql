{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}
select
    order_id,
    customer_id,
    order_date,
    order_amount,
    order_status,
    last_updated
from {{ ref('stg_orders') }}
{% if is_incremental() %}
where last_updated > (select max(last_updated) from {{ this }})
{% endif %}