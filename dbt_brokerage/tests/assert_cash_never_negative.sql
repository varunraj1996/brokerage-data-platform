-- Cash proof (part 1): an account's cash balance never ends a day below zero.
-- Balance = running total of every cash movement (deposits, withdrawals, trades, fees) up to and including that day.
with daily as (
    select account_id, movement_date, sum(amount) as net_flow
    from {{ ref('fct_cash_movements') }}
    group by account_id, movement_date
)

select account_id, movement_date, cash_balance
from (
    select account_id, movement_date,
           sum(net_flow) over (partition by account_id order by movement_date
                               rows between unbounded preceding and current row) as cash_balance
    from daily
)
where cash_balance < -0.005
