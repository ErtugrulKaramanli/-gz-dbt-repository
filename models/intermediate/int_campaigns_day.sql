with campaigns as (
    select * from {{ ref('int_campaigns') }}
),

daily_summary as (
    select
        date_date,
        paid_source,
        campaign_key,
        campaign_name,
        -- Metrikleri topluyoruz (Aggregate)
        sum(ads_cost) as ads_cost,
        sum(impression) as impression,
        sum(click) as click
    from campaigns
    group by 
        date_date,
        paid_source,
        campaign_key,
        campaign_name
)

select * from daily_summary