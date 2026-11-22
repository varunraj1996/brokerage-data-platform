-- Positions → trades reconciliation: for every account × stock, the shares held on the LAST trading day
-- must equal all its trades netted together. Computed two independent ways; any difference is returned.
with from_trades as (
    select account_id, ticker, sum(signed_quantity) as shares_from_trades
    from {{ ref('fct_trades') }}
    group by account_id, ticker
),

last_day as (
    select max(calendar_date) as d from {{ ref('dim_date') }} where is_trading_day
),

from_positions as (
    select p.account_id, p.ticker, p.shares_held as shares_from_positions
    from {{ ref('fct_daily_positions') }} p
    join last_day l on p.position_date = l.d
)

select t.account_id, t.ticker, t.shares_from_trades, coalesce(p.shares_from_positions, 0) as shares_from_positions
from from_trades t
left join from_positions p on p.account_id = t.account_id and p.ticker = t.ticker
where t.shares_from_trades <> coalesce(p.shares_from_positions, 0)
