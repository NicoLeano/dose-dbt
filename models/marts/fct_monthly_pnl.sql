{{ config(materialized='table') }}

-- Monthly P&L summary across all platforms

with daily as (
    select * from {{ ref('fct_daily_pnl') }}
),

monthly as (
    select
        date_trunc('month', date)::date as month,
        platform,
        sum(gross_revenue) as gross_revenue,
        sum(discounts) as discounts,
        sum(net_revenue) as net_revenue,
        sum(revenue_ex_iva) as revenue_ex_iva,
        sum(iva_collected) as iva_collected,
        sum(orders) as orders,
        sum(meta_ad_spend) as meta_ad_spend,
        -- Calculate AOV
        case when sum(orders) > 0
             then round(sum(net_revenue) / sum(orders), 2)
             else 0 end as aov
    from daily
    group by date_trunc('month', date), platform
),

totals as (
    select
        month,
        'all_platforms' as platform,
        sum(gross_revenue) as gross_revenue,
        sum(discounts) as discounts,
        sum(net_revenue) as net_revenue,
        sum(revenue_ex_iva) as revenue_ex_iva,
        sum(iva_collected) as iva_collected,
        sum(orders) as orders,
        sum(meta_ad_spend) as meta_ad_spend,
        case when sum(orders) > 0
             then round(sum(net_revenue) / sum(orders), 2)
             else 0 end as aov
    from monthly
    group by month
)

select * from monthly
union all
select * from totals
order by month desc, platform
