-- Staging: one row per client (current version; full history arrives in slice 3).
select
    client_id,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as client_name,
    email          as client_email,
    phone          as client_phone,
    ssn,                                -- PII: masked in gold later
    date_of_birth,
    state,
    risk_profile,
    segment,
    advisor_id,
    onboarded_date,
    _silver_loaded_at as loaded_at
from {{ source('silver', 'clients') }}
