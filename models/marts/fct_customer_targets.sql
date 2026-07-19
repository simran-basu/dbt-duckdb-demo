{{ config(materialized='table') }}

with alerts as (
    select * from {{ ref('int_customer_alerts') }}
),
tiered as (
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
        alert_priority,
        -- simplified opportunity tiering, mirroring a tiered target list
        case
            when alert_priority = 1 then 'Tier 1 - High Priority'
            when alert_priority = 2 then 'Tier 2 - Medium Priority'
            when alert_priority = 3 then 'Tier 3 - Low Priority'
            else 'Tier 4 - No Action'
        end as target_tier
    from alerts
)
select * from tiered
order by alert_priority, total_spend desc