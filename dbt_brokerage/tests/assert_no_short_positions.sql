-- Business rule: an account can never hold a negative number of shares.
-- Walk each account's trades in order and keep a running share count per stock; return any point where it dips below 0.
select account_id, ticker, trade_id, executed_at, shares_held_after
from (
    select account_id, ticker, trade_id, executed_at,
           sum(signed_quantity) over (partition by account_id, ticker
                                      order by executed_at, trade_id
                                      rows between unbounded preceding and current row) as shares_held_after
    from {{ ref('fct_trades') }}
)
where shares_held_after < 0
