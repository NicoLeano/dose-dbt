{{ config(materialized='view') }}

-- Extract refunds from Shopify order_refunds table
-- Returns are tracked as positive amounts (will be subtracted in P&L)

with source as (
    select * from {{ source('raw', 'order_refunds') }}
),

refund_line_items as (
    select
        id as refund_id,
        order_id,
        ((created_at::timestamp - interval '6 hours'))::date as refund_date,
        jsonb_array_elements(refund_line_items::jsonb) as line_item
    from source
),

parsed as (
    select
        refund_id,
        order_id,
        refund_date,
        (line_item->>'subtotal')::numeric as refund_subtotal,
        (line_item->>'total_tax')::numeric as refund_tax,
        (line_item->>'quantity')::int as quantity
    from refund_line_items
)

select
    refund_id,
    order_id,
    refund_date,
    sum(refund_subtotal) as refund_amount,
    sum(refund_tax) as refund_tax,
    sum(quantity) as units_refunded,
    'shopify' as platform
from parsed
group by refund_id, order_id, refund_date
