{{ config(materialized='view') }}

-- Convert UTC timestamps to Mexico timezone (UTC-6) for correct date attribution
-- See: [[UTC date parsing shifts Mexico dates back one day]]
--
-- Gross Revenue = subtotal + shipping (discounts tracked separately)
-- Only paid orders included (paid + partially_refunded per sync sheet mapping)
-- Pending orders auto-included once paid via Airbyte incremental sync (uses updated_at cursor)

with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        id as order_id,
        order_number,
        ((created_at::timestamp - interval '6 hours'))::date as order_date,
        financial_status as status,
        -- Gross Revenue = subtotal + shipping (discounts separate)
        coalesce(subtotal_price::numeric, 0) +
            coalesce((total_shipping_price_set->'shop_money'->>'amount')::numeric, 0) as gross_revenue,
        coalesce(total_discounts::numeric, 0) as discounts,
        coalesce((total_shipping_price_set->'shop_money'->>'amount')::numeric, 0) as shipping,
        'shopify' as platform,
        'shopify:' || id::text as source_row_id
    from source
    where cancelled_at is null
      and financial_status in ('paid', 'partially_refunded')
)

select * from renamed
