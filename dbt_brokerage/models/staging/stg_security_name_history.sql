-- Staging: legal-name periods per company, already contiguous and non-overlapping (built in notebook 09).
select
    cik,
    legal_name,
    valid_from,
    valid_to,
    is_current
from {{ source('silver', 'security_name_history') }}
