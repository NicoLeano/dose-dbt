{{ config(materialized='view') }}

-- Extract line items from Shopify orders for COGS calculation
-- Product costs (MXN):
--   Calm Cacao: 129
--   Mushroom Coffee: 100
--   Collagen Creamer: 125

with source as (
    select * from {{ source('raw', 'orders') }}
    where cancelled_at is null
      and financial_status in ('paid', 'partially_refunded')
),

line_items_expanded as (
    select
        id as order_id,
        ((created_at::timestamp - interval '6 hours'))::date as order_date,
        jsonb_array_elements(line_items::jsonb) as line_item
    from source
),

parsed as (
    select
        order_id,
        order_date,
        line_item->>'title' as product_title,
        line_item->>'sku' as sku,
        (line_item->>'quantity')::int as quantity,
        (line_item->>'price')::numeric as unit_price
    from line_items_expanded
),

with_costs as (
    select
        order_id,
        order_date,
        product_title,
        sku,
        quantity,
        unit_price,
        -- Map SKUs to COGS per unit (MXN)
        case
            when sku in ('doseofcacao-1', 'doseofcacao') then 129      -- Calm Cacao
            when sku in ('doseofcoffee-1', 'doseofcoffee') then 100    -- Mushroom Coffee
            when sku in ('doseofcreamer-1', 'doseofcreamer') then 125  -- Collagen Creamer
            when sku = 'starterkit-1' then 354                         -- Kit: 1 of each (129+100+125)
            when sku = 'kitdiaynoche-1' then 254                       -- Day & Night: cacao + coffee (129+125)
            when sku = 'kitenergia-1' then 225                         -- Energy: coffee + creamer (100+125)
            else 0  -- Unknown products
        end as cost_per_unit,
        'shopify' as platform
    from parsed
)

select
    order_id,
    order_date,
    product_title,
    sku,
    quantity,
    unit_price,
    cost_per_unit,
    quantity * cost_per_unit as line_cogs,
    platform
from with_costs
