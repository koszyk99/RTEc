{{ config(materialized='table') }}

select
    product_category,
    count(order_id) as total_orders,
    round(sum(price)::numeric, 2) as total_revenue,
    round(avg(price)::numeric, 2) as avg_orders_value
from {{ ref('stg_orders') }}
group by 1
order by total_revenue desc
