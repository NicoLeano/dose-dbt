{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'meta_ads_insights') }}
),

renamed as (
    select
        ad_id,
        date_start::date as insight_date,
        spend::numeric as spend,
        impressions::int as impressions,
        clicks::int as clicks,
        reach::int as reach,
        cpm::numeric as cpm,
        cpc::numeric as cpc,
        ctr::numeric as ctr,
        -- Extract conversions from actions array
        (select sum((a->>'value')::numeric)
         from jsonb_array_elements(actions) a
         where a->>'action_type' = 'purchase') as conversions,
        (select sum((a->>'value')::numeric)
         from jsonb_array_elements(action_values) a
         where a->>'action_type' = 'purchase') as conversion_value
    from source
)

select * from renamed
