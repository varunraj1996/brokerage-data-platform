-- Cash proof (part 2): opening cash + all movements = closing cash, for every account.
-- The running balance in fct_daily_account_value is compared with a fresh, independent sum of fct_cash_movements.
with last_day as (
    select account_id, max(value_date) as d from {{ ref('fct_daily_account_value') }} group by account_id
),

closing as (
    select v.account_id, v.cash_balance
    from {{ ref('fct_daily_account_value') }} v
    join last_day l on l.account_id = v.account_id and l.d = v.value_date
),

movements as (
    select account_id, sum(amount) as all_movements
    from {{ ref('fct_cash_movements') }}
    group by account_id
)

select c.account_id, c.cash_balance as closing_cash, m.all_movements
from closing c
join movements m using (account_id)
where abs(c.cash_balance - m.all_movements) > 0.005
