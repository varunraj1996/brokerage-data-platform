-- dim_advisor · grain: one row per advisor · SCD1 (changes overwrite, no history)
select
    {{ dbt_utils.generate_surrogate_key(['advisor_id']) }} as advisor_sk,
    advisor_id,
    advisor_name,
    advisor_email,
    advisor_team,
    hired_date
from {{ ref('stg_advisors') }}
