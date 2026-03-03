{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'rawads_insightsf1bcd7014f92bb079e62eb151ae93bb3') }}
),

renamed as (
    select
        ad_id,
        (date_start::timestamp)::date as insight_date,
        coalesce(spend::numeric, 0) as spend,
        coalesce(impressions::int, 0) as impressions,
        coalesce(clicks::int, 0) as clicks,
        coalesce(reach::int, 0) as reach,
        coalesce(cpm::numeric, 0) as cpm,
        coalesce(cpc::numeric, 0) as cpc,
        coalesce(ctr::numeric, 0) as ctr,
        -- Extract conversions from actions array if present
        coalesce(
            (select sum((a->>'value')::numeric)
             from jsonb_array_elements(actions::jsonb) a
             where a->>'action_type' = 'purchase'), 0
        ) as conversions,
        coalesce(
            (select sum((a->>'value')::numeric)
             from jsonb_array_elements(action_values::jsonb) a
             where a->>'action_type' = 'purchase'), 0
        ) as conversion_value
    from source
    where spend is not null
)

select * from renamed
