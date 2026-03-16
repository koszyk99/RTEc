with source as (
    select * from {{ source("raw_data", "raw_orders") }}
),

renamed as (
    select
        order_id,
        user_id,
        product as product_name,
        category as product_category,
        price::float as price,
        timestamp::timestamp as order_timestamp
    from source
)

select * from renamed
