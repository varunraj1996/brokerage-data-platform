-- Stitches two sources of history into one timeline per company:
--   · before the first snapshot: SEC former-name periods (silver), with the attributes seen in the first snapshot
--   · from the first snapshot on: the versions recorded by the dbt snapshot snp_security
with snap as (
    select
        cik, ticker, display_name, legal_name, exchange, sic_code, sic_description, sector, is_benchmark,
        to_date(dbt_valid_from) as snap_from,
        to_date(dbt_valid_to)   as snap_to
    from {{ ref('snp_security') }}
),

first_snap as (
    select * from (
        select snap.*, row_number() over (partition by cik order by snap_from) as rn
        from snap
    ) where rn = 1
),

backfill as (
    select
        h.cik, f.ticker, f.display_name, h.legal_name, f.exchange, f.sic_code, f.sic_description,
        f.sector, f.is_benchmark,
        h.valid_from,
        least(h.valid_to, date_sub(f.snap_from, 1)) as valid_to,
        'sec_name_history' as version_source
    from {{ ref('stg_security_name_history') }} h
    join first_snap f on f.cik = h.cik
    where h.valid_from < f.snap_from
),

tracked as (
    select
        cik, ticker, display_name, legal_name, exchange, sic_code, sic_description, sector, is_benchmark,
        snap_from as valid_from,
        coalesce(date_sub(snap_to, 1), date'9999-12-31') as valid_to,
        'dbt_snapshot' as version_source
    from snap
)

select * from backfill
union all
select * from tracked where valid_to >= valid_from     -- drops versions that lasted less than a day
