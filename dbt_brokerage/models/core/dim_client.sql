-- dim_client · grain: one row per client (current version)
-- Becomes SCD2 in slice 3, built from the SQL Server temporal history.
select
    {{ dbt_utils.generate_surrogate_key(['c.client_id']) }} as client_sk,
    c.client_id,
    c.client_name,
    c.client_email,
    c.client_phone,
    c.ssn,
    c.date_of_birth,
    floor(months_between(current_date(), c.date_of_birth) / 12) as age_years,
    c.state,
    c.risk_profile,
    c.segment,
    c.onboarded_date,
    c.advisor_id,
    a.advisor_name,
    a.advisor_team
from {{ ref('stg_clients') }} c
left join {{ ref('stg_advisors') }} a using (advisor_id)
