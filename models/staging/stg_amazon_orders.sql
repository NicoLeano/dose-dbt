{{ config(materialized='view') }}

-- Amazon Seller data from Airbyte (fulfilled shipments report)
-- Each row is a shipment line item
-- Convert UTC timestamps to Mexico timezone (UTC-6)

with source as (
    select * from {{ source('raw', 'GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL') }}
),

renamed as (
    select
        amazon_order_id as order_id,
        ((purchase_date::timestamp - interval '6 hours'))::date as order_date,
        'Shipped' as status,
        coalesce(item_price::numeric, 0) as gross_revenue,
        coalesce(item_promotion_discount::numeric, 0) as discounts,
        'amazon' as platform,
        'amazon:' || amazon_order_id || ':' || coalesce(amazon_order_item_id, '') as source_row_id
    from source
    where amazon_order_id is not null
)

select * from renamed
