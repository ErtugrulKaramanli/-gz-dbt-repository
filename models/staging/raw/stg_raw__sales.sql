with 

source as (

    select * from {{ source('raw', 'sales') }}

),

renamed as (

    select
        concat(orders_id, '_', pdt_id) as sales_id,
        date_date,
        orders_id,
        pdt_id as products_id,
        revenue,
        quantity

    from source

)

select * from renamed