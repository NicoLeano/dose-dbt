-- ROAS should be non-negative
select *
from {{ ref('fct_creative_performance') }}
where roas < 0
