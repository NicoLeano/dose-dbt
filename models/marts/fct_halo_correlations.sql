{{ config(materialized='table') }}

-- This model calculates cross-channel correlation coefficients
-- to measure the "halo effect" of paid media on marketplace sales

with daily_meta as (
    select
        insight_date as date,
        sum(spend) as meta_spend
    from {{ ref('stg_meta_insights') }}
    group by insight_date
),

daily_amazon as (
    select
        order_date as date,
        sum(gross_revenue) as amazon_revenue,
        count(*) as amazon_orders
    from {{ ref('stg_amazon_orders') }}
    group by order_date
),

daily_meli as (
    select
        order_date as date,
        sum(gross_revenue) as meli_revenue,
        count(*) as meli_orders
    from {{ ref('stg_mercadolibre_orders') }}
    group by order_date
),

-- Calculate correlations for different lag periods
correlations as (
    -- Meta -> Amazon (3-day lag)
    select
        'meta_ads' as source_channel,
        'amazon' as target_channel,
        3 as lag_days,
        corr(m.meta_spend, a.amazon_revenue) as correlation,
        regr_slope(a.amazon_revenue, m.meta_spend) as dollar_impact
    from daily_meta m
    join daily_amazon a on a.date = m.date + interval '3 days'
    where m.meta_spend > 0

    union all

    -- Meta -> Amazon (7-day lag)
    select
        'meta_ads' as source_channel,
        'amazon' as target_channel,
        7 as lag_days,
        corr(m.meta_spend, a.amazon_revenue) as correlation,
        regr_slope(a.amazon_revenue, m.meta_spend) as dollar_impact
    from daily_meta m
    join daily_amazon a on a.date = m.date + interval '7 days'
    where m.meta_spend > 0

    union all

    -- Meta -> MercadoLibre (5-day lag)
    select
        'meta_ads' as source_channel,
        'mercadolibre' as target_channel,
        5 as lag_days,
        corr(m.meta_spend, l.meli_revenue) as correlation,
        regr_slope(l.meli_revenue, m.meta_spend) as dollar_impact
    from daily_meta m
    join daily_meli l on l.date = m.date + interval '5 days'
    where m.meta_spend > 0

    union all

    -- Meta -> MercadoLibre (7-day lag)
    select
        'meta_ads' as source_channel,
        'mercadolibre' as target_channel,
        7 as lag_days,
        corr(m.meta_spend, l.meli_revenue) as correlation,
        regr_slope(l.meli_revenue, m.meta_spend) as dollar_impact
    from daily_meta m
    join daily_meli l on l.date = m.date + interval '7 days'
    where m.meta_spend > 0
)

select
    source_channel,
    target_channel,
    lag_days,
    round(correlation::numeric, 2) as correlation,
    round(dollar_impact::numeric, 2) as dollar_impact,
    case
        when abs(correlation) >= 0.7 then 'strong'
        when abs(correlation) >= 0.4 then 'moderate'
        else 'weak'
    end as strength,
    current_timestamp as calculated_at
from correlations
where correlation is not null
order by abs(correlation) desc
