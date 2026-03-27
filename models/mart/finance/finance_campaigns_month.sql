with daily_finance as (
    select * from {{ ref('finance_campaigns_day') }}
),

monthly_aggregation as (
    select
        -- Tarihi ayın ilk gününe yuvarlıyoruz (Örn: 2026-03-01)
        date_trunc(date, month) as datemonth,
        -- Finansal Metrikler (Toplam)
        round(sum(ads_margin), 2) as ads_margin,
        round(sum(operational_margin), 2) as operational_margin,
        round(sum(ads_cost), 2) as ads_cost,
        sum(ads_impression) as ads_impression,
        sum(ads_click) as ads_clicks, -- Sütun adını isteğine göre ads_clicks yaptık
        sum(quantity) as quantity,
        round(sum(revenue), 2) as revenue,
        round(sum(purchase_cost), 2) as purchase_cost,
        round(sum(margin), 2) as margin,
        round(sum(shipping_fee), 2) as shipping_fee,
        round(sum(log_cost), 2) as log_cost,
        round(sum(ship_cost), 2) as ship_cost,
        -- Ortalama Sepet Tutarı (Aylık toplam ciro / aylık toplam adet veya işlem üzerinden hesaplanmalı)
        -- Burada günlük ortalamaların ortalamasını almak yerine, aylık toplamlar üzerinden hesaplamak daha doğrudur:
        round(safe_divide(sum(revenue), sum(quantity)), 2) as average_basket
    from daily_finance
    group by datemonth
)

select * from monthly_aggregation
order by datemonth desc