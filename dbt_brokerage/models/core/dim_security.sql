-- dim_security · grain: one row per company version · SCD2, keyed on the SEC's CIK
select
    {{ dbt_utils.generate_surrogate_key(['cik', 'valid_from']) }} as security_sk,
    cik,
    ticker,
    display_name,
    legal_name,
    trim(regexp_replace(legal_name, '\\s*/[A-Za-z]+/?\\s*$', '')) as legal_name_clean,   -- drops "/DE/", "/NEW"…
    exchange,
    sic_code,
    sic_description,
    sector,
    is_benchmark,
    valid_from,
    valid_to,
    valid_to = date'9999-12-31' as is_current,
    version_source
from {{ ref('int_security_history') }}
