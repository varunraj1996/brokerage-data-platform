-- mart_live_holdings_value · grain: one row per stock the firm's clients hold
-- What are our clients' holdings worth right now?
--   shares held  = all trades netted (end of the trade history)
--   value then   = shares × last historical close (split-adjusted, from fct_daily_prices)
--   value now    = shares × latest live price (from silver.live_bars_1m, filled while notebooks 14–15 run)
-- A view, so every query sees the newest live bar.
with holdings as (
    select ticker, sum(signed_quantity) as shares_held
    from {{ ref('fct_trades') }}
    group by ticker
    having sum(signed_quantity) > 0
),

last_close as (
    select ticker, trade_date as last_close_date, split_adjusted_close as last_close
    from {{ ref('fct_daily_prices') }}
    where trade_date = (select max(trade_date) from {{ ref('fct_daily_prices') }})
),

live as (
    select symbol                         as ticker,
           max_by(close, bar_start)       as live_price,
           max(bar_end)                   as live_as_of
    from {{ source('silver', 'live_bars_1m') }}
    group by symbol
)

select
    h.ticker,
    h.shares_held,
    c.last_close_date,
    c.last_close,
    round(h.shares_held * c.last_close, 2)                         as value_at_last_close,
    l.live_price,
    l.live_as_of,
    round(h.shares_held * l.live_price, 2)                         as live_value,
    round(h.shares_held * (l.live_price - c.last_close), 2)        as change_since_last_close,
    round(l.live_price / c.last_close - 1, 4)                      as pct_change
from holdings h
join last_close c using (ticker)
left join live l using (ticker)          -- null live price = no live tick for this stock in any session yet
