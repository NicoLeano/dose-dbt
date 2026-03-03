-- Correlation coefficient must be between -1 and 1
select *
from {{ ref('fct_halo_correlations') }}
where correlation < -1 or correlation > 1
