with 

source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        products_id,
        SAFE_CAST(purchSE_PRICE as FLOAT64) as purchase_price

    from source

)

select * from renamed