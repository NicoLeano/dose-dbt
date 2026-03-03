{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'amazon_orders') }}
),

renamed as (
    select
        amazon_order_id as order_id,
        purchase_date::date as order_date,
        order_status as status,
        order_total_amount::numeric as gross_revenue,
        0::numeric as discounts,  -- Amazon handles discounts differently
        'amazon' as platform,
        'amazon:' || amazon_order_id as source_row_id
    from source
    where order_status not in ('Cancelled', 'Pending')
)

select * from renamed
