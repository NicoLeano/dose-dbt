{{ config(materialized='table') }}

-- Daily P&L combining all platforms

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

meli_daily as (
    select
        order_date as date,
        sum(gross_revenue) as gross_revenue,
        sum(discounts) as discounts,
        count(*) as orders,
        'mercadolibre' as platform
    from {{ ref('stg_mercadolibre_orders') }}
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
    select * from shopify_daily
    union all
    select * from amazon_daily
    union all
    select * from meli_daily
),

aggregated as (
    select
        r.date,
        r.platform,
        r.gross_revenue,
        r.discounts,
        r.orders,
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
    coalesce(m.ad_spend, 0) as meta_ad_spend
from aggregated a
left join meta_daily m on a.date = m.date and a.platform = 'shopify'
order by a.date desc, a.platform
