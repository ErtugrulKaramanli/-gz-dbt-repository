with orders_operational as (
    select * from {{ ref('int_orders_margin') }}
),

shipping as (
    select * from {{ ref('stg_raw__ship') }}
)

select
    o.orders_id,
    o.date_date,
    o.revenue,
    o.quantity,
    o.purchase_cost,
    o.margin,
    s.shipping_fee,
    s.logcost,
    s.ship_cost,
    ROUND(o.margin + s.shipping_fee - s.logcost - s.ship_cost,2) as operational_margin
from orders_operational as o
left join shipping as s on o.orders_id = s.orders_id
ORDER BY orders_id desc





