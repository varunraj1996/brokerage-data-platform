-- Gate gold → marts: firm-wide AUM per day in the mart = the sum of every account's value that day.
with mart as (
    select value_date, sum(aum) as aum from {{ ref('mart_aum_daily') }} group by value_date
),

accounts as (
    select value_date, sum(total_value) as aum from {{ ref('fct_daily_account_value') }} group by value_date
)

select coalesce(m.value_date, a.value_date) as value_date, m.aum as mart_aum, a.aum as accounts_aum
from mart m
full outer join accounts a on a.value_date = m.value_date
where m.aum is null or a.aum is null or abs(m.aum - a.aum) > 0.01
