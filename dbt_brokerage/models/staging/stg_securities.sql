-- Staging: one row per universe ticker, with its current SEC identity.
select
    ticker,
    cik,
    display_name,                 -- clean name from ref_universe, for reports
    legal_name,                   -- as filed with the SEC
    exchange,
    sic_code,
    sic_description,
    entity_type,
    sector,
    is_benchmark
from {{ source('silver', 'securities') }}
