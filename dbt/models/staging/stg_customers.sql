with source as (
    select * from {{ ref('customers') }}
),
renamed as (
    select
        customer_id,
        name as customer_name,
        signup_date,
        lower(region) as region
    from source
)
select * from renamed