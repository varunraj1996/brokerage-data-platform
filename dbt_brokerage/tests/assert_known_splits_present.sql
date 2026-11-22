-- Known real splits in 2024 must appear with the right ratio on the right ex-date.
-- A hand-checked "golden" list: if the source, the silver logic or this model ever loses or shifts a split, this fails.
with expected as (
    select * from values
        ('WMT',  date'2024-02-26',  3.0),
        ('NVDA', date'2024-06-10', 10.0),
        ('CMG',  date'2024-06-26', 50.0),
        ('AVGO', date'2024-07-15', 10.0)
    as t(ticker, ex_date, split_ratio)
)

select e.ticker, e.ex_date, e.split_ratio as expected_ratio, a.split_ratio as found_ratio
from expected e
left join {{ ref('fct_corporate_actions') }} a
    on a.ticker = e.ticker and a.ex_date = e.ex_date and a.action_type = 'SPLIT'
where a.split_ratio is null or a.split_ratio <> e.split_ratio
