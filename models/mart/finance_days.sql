with orders as (
    select * from {{ ref('int_orders_operational') }}
)

select
    date_date,
    cast(count(orders_id) as int64) as number_of_transactions,
    round(sum(revenue), 2) as revenue,
    -- Division (bölme) işlemini ayrı bir paranteze alalım
    round(safe_divide(sum(revenue), count(orders_id)), 2) as average_basket,
    round(sum(operational_margin), 2) as operational_margin,
    round(sum(purchase_cost), 2) as total_purchase_cost,
    round(sum(shipping_fee), 2) as total_shipping_fees,
    round(sum(logcost), 2) as total_logistics_costs,
    cast(sum(quantity) as int64) as total_quantity_sold
from orders
group by date_date
order by date_date desc