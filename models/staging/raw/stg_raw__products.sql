with 

source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        products_id,
        SAFE_CAST(purchase_price as FLOAT64)

    from source

)

select * from renamed