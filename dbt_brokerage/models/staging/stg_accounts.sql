-- Staging: one row per account.
select
    account_id,
    account_number,                     -- PII: masked in gold later
    client_id,
    account_type,
    status         as account_status,
    opened_date,
    closed_date,
    _silver_loaded_at as loaded_at
from {{ source('silver', 'accounts') }}
