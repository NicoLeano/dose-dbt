{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'mercadolibre_orders') }}
),

renamed as (
    select
        id::text as order_id,
        date_created::date as order_date,
        status,
        total_amount::numeric as gross_revenue,
        0::numeric as discounts,
        'mercadolibre' as platform,
        'meli:' || id::text as source_row_id
    from source
    where status not in ('cancelled')
)

select * from renamed
