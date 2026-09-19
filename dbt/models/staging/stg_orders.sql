with source as (
    select * from {{ ref('orders') }}
),
renamed as (
    select
        order_id,
        customer_id,
        order_date,
        order_amount,
        lower(status) as order_status,
        last_updated
    from source
)
select * from renamed