with adwords as (
    select * from {{ ref('stg_raw__adwords') }}
),

bing as (
    select * from {{ ref('stg_raw__bing') }}
),

criteo as (
    select * from {{ ref('stg_raw__criteo') }}
),

facebook as (
    select * from {{ ref('stg_raw__facebook') }}
),

-- Tüm kanalları alt alta birleştiriyoruz
unioned as (
    select * from adwords
    union all
    select * from bing
    union all
    select * from criteo
    union all
    select * from facebook
)

select * from unioned