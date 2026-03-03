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
        id as ad_id,
        creative->>'id' as creative_id
    from {{ source('raw', 'rawadsf52608e9b8e7ffa9ed8b2b2dba082626') }}
    where creative is not null
),

joined as (
    select
        c.creative_id,
        c.creative_name,
        c.thumbnail_url,
        c.creative_type,
        coalesce(i.total_spend, 0) as spend,
        coalesce(i.total_impressions, 0) as impressions,
        coalesce(i.total_clicks, 0) as clicks,
        coalesce(i.total_conversions, 0) as conversions,
        coalesce(i.total_revenue, 0) as revenue,
        -- Calculated metrics
        case when coalesce(i.total_spend, 0) > 0
             then round(coalesce(i.total_revenue, 0) / i.total_spend, 2)
             else 0 end as roas,
        case when coalesce(i.total_conversions, 0) > 0
             then round(i.total_spend / i.total_conversions, 2)
             else 0 end as cpa,
        case when coalesce(i.total_impressions, 0) > 0
             then round(i.total_clicks::numeric / i.total_impressions * 100, 2)
             else 0 end as ctr,
        i.first_active,
        i.last_active,
        case when i.last_active >= current_date - interval '7 days'
             then 'active' else 'inactive' end as status
    from creatives c
    left join ads_to_creatives atc on c.creative_id = atc.creative_id
    left join insights i on atc.ad_id = i.ad_id
    where c.creative_id is not null
)

select * from joined
order by spend desc
