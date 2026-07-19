with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
order_signals as (
    select
        customer_id,
        count(*) as total_orders,
        sum(order_amount) as total_spend,
        sum(case when order_status = 'pending' then 1 else 0 end) as pending_orders,
        sum(case when order_status = 'cancelled' then 1 else 0 end) as cancelled_orders,
        max(order_date) as last_order_date
    from orders
    group by customer_id
),
consolidated as (
    select
        c.customer_id,
        c.customer_name,
        c.region,
        coalesce(o.total_orders, 0) as total_orders,
        coalesce(o.total_spend, 0) as total_spend,
        coalesce(o.pending_orders, 0) as pending_orders,
        coalesce(o.cancelled_orders, 0) as cancelled_orders,
        o.last_order_date,
        -- simplified rule-based alert consolidation
        case
            when coalesce(o.total_spend, 0) > 400 then 'high_value_customer'
            when coalesce(o.pending_orders, 0) > 0 then 'pending_order_followup'
            when coalesce(o.cancelled_orders, 0) > 0 then 'cancellation_review'
            else 'no_alert'
        end as alert_reason,
        case
            when coalesce(o.total_spend, 0) > 400 then 1
            when coalesce(o.pending_orders, 0) > 0 then 2
            when coalesce(o.cancelled_orders, 0) > 0 then 3
            else 4
        end as alert_priority
    from customers c
    left join order_signals o
        on c.customer_id = o.customer_id
)
select * from consolidated