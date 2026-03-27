with sales as (
    select * from {{ ref('stg_raw__sales') }}
),

products as (
    select * from {{ ref('stg_raw__products') }}
),

joined as (
    -- Sales ve Products tablolarını products_id üzerinden birleştiriyoruz
    select
        s.*,
        p.purchase_price
    from sales s
    left join products p on s.products_id = p.products_id
),

calculations as (
    -- Marj ve maliyet hesaplamalarını yapıyoruz
    select
        *,
        cast(quantity as float64) * cast(purchase_price as float64) as purchase_cost,
        ROUND(cast(revenue as float64) - (cast(quantity as float64) * cast(purchase_price as float64)),2) as margin
    from joined
)

select * from calculations