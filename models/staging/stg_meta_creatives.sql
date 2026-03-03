{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'rawad_creativescbd25427cd3a238158c64d74da2fd4a8') }}
),

renamed as (
    select
        id as creative_id,
        name as creative_name,
        thumbnail_url,
        object_type as creative_type,
        body as creative_body,
        title as creative_title,
        call_to_action_type as cta_type,
        _airbyte_extracted_at as synced_at
    from source
)

select * from renamed
