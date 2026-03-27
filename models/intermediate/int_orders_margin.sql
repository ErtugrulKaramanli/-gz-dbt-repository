with sales_margin_data as (
    -- Bir önceki adımda oluşturduğun modeli referans alıyorsun
    select * from {{ ref('int_sales_margin') }}
)

select
    orders_id,
    date_date,
    ROUND(SUM(revenue),2) as revenue,
    ROUND(SUM(quantity),2) as quantity,
    ROUND(SUM(purchase_cost),2) as purchase_cost,
    ROUND(SUM(margin),2) as margin
from sales_margin_data
GROUP BY orders_id, date_date
ORDER BY orders_id desc