{{ config(materialized='table') }}

with creatives as (
    select * from {{ ref('stg_meta_creatives') }}
),

insights as (
    select
        ad_id,
        sum(spend) as total_spend,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        sum(conversions) as total_conversions,
        sum(conversion_value) as total_revenue,
        min(insight_date) as first_active,
        max(insight_date) as last_active
    from {{ ref('stg_meta_insights') }}
    group by ad_id
),

ads_to_creatives as (
    select
        ad.id as ad_id,
        ad.creative_id
    from {{ source('raw', 'meta_ads') }} ad
),

joined as (
    select
        c.creative_id,
        c.creative_name,
        c.thumbnail_url,
        c.creative_type,
        i.total_spend as spend,
        i.total_impressions as impressions,
        i.total_clicks as clicks,
        i.total_conversions as conversions,
        i.total_revenue as revenue,
        -- Calculated metrics
        case when i.total_spend > 0
             then round(i.total_revenue / i.total_spend, 2)
             else 0 end as roas,
        case when i.total_conversions > 0
             then round(i.total_spend / i.total_conversions, 2)
             else 0 end as cpa,
        case when i.total_impressions > 0
             then round(i.total_clicks::numeric / i.total_impressions * 100, 2)
             else 0 end as ctr,
        i.first_active,
        i.last_active,
        case when i.last_active >= current_date - interval '7 days'
             then 'active' else 'inactive' end as status
    from creatives c
    join ads_to_creatives atc on c.creative_id = atc.creative_id
    join insights i on atc.ad_id = i.ad_id
)

select * from joined
order by spend desc
