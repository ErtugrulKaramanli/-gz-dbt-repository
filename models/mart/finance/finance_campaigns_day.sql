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
        f.quantity,
        f.revenue,
        f.purchase_cost,
        f.margin,
        f.shipping_fee,
        f.logCost as log_cost,
        f.ship_cost
    from finance_days f
    left join campaigns_day c 
        on f.date_date = c.date_date
)

select * from joined