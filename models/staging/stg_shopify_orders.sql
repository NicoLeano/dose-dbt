{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'shopify_orders') }}
),

renamed as (
    select
        id as order_id,
        order_number,
        created_at::date as order_date,
        financial_status as status,
        total_price::numeric as gross_revenue,
        total_discounts::numeric as discounts,
        'shopify' as platform,
        'shopify:' || id::text as source_row_id
    from source
    where cancelled_at is null
      and financial_status = 'paid'
)

select * from renamed
