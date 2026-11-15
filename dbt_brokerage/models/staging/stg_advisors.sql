-- Staging: one row per advisor, business columns only, consistent names.
select
    advisor_id,
    full_name      as advisor_name,
    email          as advisor_email,
    team           as advisor_team,
    hired_date,
    _silver_loaded_at as loaded_at
from {{ source('silver', 'advisors') }}
