-- Every company has exactly one current version in dim_security.
select cik, sum(case when is_current then 1 else 0 end) as current_versions
from {{ ref('dim_security') }}
group by cik
having sum(case when is_current then 1 else 0 end) <> 1
