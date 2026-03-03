{{ config(materialized='table') }}

-- Daily P&L combining Shopify and Amazon (MercadoLibre via legacy system)

with shopify_daily as (
    select
        order_date as date,
        sum(gross_revenue) as gross_revenue,
        sum(discounts) as discounts,
        count(*) as orders,
        'shopify' as platform
    from {{ ref('stg_shopify_orders') }}
    group by order_date
),

shopify_cogs as (
    select
        order_date as date,
        sum(line_cogs) as cogs
    from {{ ref('stg_shopify_line_items') }}
    group by order_date
),

amazon_daily as (
    select
        order_date as date,
        sum(gross_revenue) as gross_revenue,
        sum(discounts) as discounts,
        count(*) as orders,
        'amazon' as platform
    from {{ ref('stg_amazon_orders') }}
    group by order_date
),

meta_daily as (
    select
        insight_date as date,
        sum(spend) as ad_spend
    from {{ ref('stg_meta_insights') }}
    group by insight_date
),

all_revenue as (
    select
        s.*,
        coalesce(c.cogs, 0) as cogs
    from shopify_daily s
    left join shopify_cogs c on s.date = c.date
    union all
    select
        a.*,
        0 as cogs  -- Amazon COGS not yet implemented
    from amazon_daily a
),

aggregated as (
    select
        r.date,
        r.platform,
        r.gross_revenue,
        r.discounts,
        r.orders,
        r.cogs,
        -- Calculate net revenue (gross - discounts)
        r.gross_revenue - r.discounts as net_revenue,
        -- IVA is 16% included in gross
        round((r.gross_revenue - r.discounts) / (1 + {{ var('iva_rate') }}), 2) as revenue_ex_iva,
        round((r.gross_revenue - r.discounts) - (r.gross_revenue - r.discounts) / (1 + {{ var('iva_rate') }}), 2) as iva_collected
    from all_revenue r
)

select
    a.date,
    a.platform,
    a.gross_revenue,
    a.discounts,
    a.net_revenue,
    a.revenue_ex_iva,
    a.iva_collected,
    a.orders,
    a.cogs,
    coalesce(m.ad_spend, 0) as meta_ad_spend
from aggregated a
left join meta_daily m on a.date = m.date and a.platform = 'shopify'
order by a.date desc, a.platform
