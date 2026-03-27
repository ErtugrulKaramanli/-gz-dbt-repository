with finance_days as (
    select * from {{ ref('finance_days') }}
),

campaigns_day as (
    select
        date_date,
        sum(ads_cost) as ads_cost,
        sum(impression) as ads_impression,
        sum(click) as ads_click
    from {{ ref('int_campaigns_day') }}
    group by date_date
),

joined as (
    select
        f.date_date as date,
        -- Finansal Hesaplama: Ads Margin
        round(f.operational_margin - coalesce(c.ads_cost, 0), 2) as ads_margin,
        f.average_basket,
        f.operational_margin,
        coalesce(c.ads_cost, 0) as ads_cost,
        coalesce(c.ads_impression, 0) as ads_impression,
        coalesce(c.ads_click, 0) as ads_click,
        -- finance_days tablandaki gerçek isimleri eşleştiriyoruz:
        f.total_quantity_sold as quantity,
        f.revenue,
        f.total_purchase_cost as purchase_cost,
        -- 'margin' finance_days içinde doğrudan yoksa operational_margin kullanılabilir 
        -- veya int_orders_operational'dan çekilebilir. Şimdilik operational_margin veriyorum:
        f.operational_margin as margin, 
        f.total_shipping_fees as shipping_fee,
        f.total_logistics_costs as log_cost,
        -- ship_cost genellikle shipping_fee + log_cost toplamıdır:
        round(f.total_shipping_fees + f.total_logistics_costs, 2) as ship_cost
    from finance_days f
    left join campaigns_day c 
        on f.date_date = c.date_date
)

select * from joined