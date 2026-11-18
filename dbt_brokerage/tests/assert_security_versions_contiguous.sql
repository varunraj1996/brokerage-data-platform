-- SCD2 integrity: each version starts the day after the previous one ends (no gaps, no overlaps),
-- so an as-of join finds exactly one version for any date.
select cik, valid_from, valid_to, next_from
from (
    select cik, valid_from, valid_to,
           lead(valid_from) over (partition by cik order by valid_from) as next_from
    from {{ ref('dim_security') }}
)
where next_from is not null and next_from <> date_add(valid_to, 1)
