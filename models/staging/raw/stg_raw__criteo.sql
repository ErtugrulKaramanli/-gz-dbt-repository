with 

source as (

    select * from {{ source('raw', 'criteo') }}

),

renamed as (

    select
        date_date,
        paid_source,
        campaign_key,
        campgn_name as campaign_name,
        cast(ads_cost as float64) as ads_cost,
        cast(impression as INT64) as impression,
        cast(click as INT64) as click

    from source

)

select * from renamed