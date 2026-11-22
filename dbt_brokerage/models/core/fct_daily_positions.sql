{{ config(
    materialized = 'table',
    liquid_clustered_by = ['position_date', 'account_id']
) }}

-- fct_daily_positions · grain: one row per account × stock × trading day while the account holds it
-- (plus the day it is sold out, so the exit is visible). A PERIODIC SNAPSHOT fact.
--
-- ⚠️ Semi-additive: shares_held and market_value can be summed across accounts or stocks on ONE day,
--    never across days (adding Monday's holdings to Tuesday's counts the same shares twice).
--
-- How it's built:
--   1. net shares traded per account × stock × day (from fct_trades)
--   2. a "spine": every trading day from the first trade in that stock onward
--   3. running total of net shares over the spine = shares held at the end of each day (carry-forward)
--   4. value at that day's split-adjusted close; dividend = shares held the day BEFORE the ex-date × dividend
with trading_days as (
    select calendar_date as position_date, date_key
    from {{ ref('dim_date') }}
    where is_trading_day
),

daily_net as (
    select account_id, ticker, trade_date as position_date,
           sum(signed_quantity) as net_shares_traded,
           count(*)             as trades_today
    from {{ ref('fct_trades') }}
    group by account_id, ticker, trade_date
),

pairs as (
    select account_id, ticker, min(position_date) as first_trade_date
    from daily_net
    group by account_id, ticker
),

spine as (
    select p.account_id, p.ticker, t.position_date, t.date_key
    from pairs p
    join trading_days t on t.position_date >= p.first_trade_date
),

running as (
    select s.*,
           coalesce(n.net_shares_traded, 0) as net_shares_traded,
           coalesce(n.trades_today, 0)      as trades_today,
           sum(coalesce(n.net_shares_traded, 0)) over (
               partition by s.account_id, s.ticker order by s.position_date
               rows between unbounded preceding and current row)               as shares_held
    from spine s
    left join daily_net n
        on n.account_id = s.account_id and n.ticker = s.ticker and n.position_date = s.position_date
),

with_prev as (
    select *,
           lag(shares_held, 1, 0) over (partition by account_id, ticker order by position_date) as shares_held_prev_day
    from running
),

dividends as (
    select ticker, ex_date, cash_per_adjusted_share
    from {{ ref('fct_corporate_actions') }}
    where action_type = 'DIVIDEND'
)

select
    r.date_key,
    a.account_sk,
    p.security_sk,
    r.account_id,
    r.ticker,
    r.position_date,
    r.shares_held,
    r.shares_held_prev_day,
    r.net_shares_traded,
    r.trades_today,
    p.split_adjusted_close                                                     as close_price,
    cast(round(r.shares_held * p.split_adjusted_close, 2) as decimal(18, 2))   as market_value,
    d.cash_per_adjusted_share                                                  as dividend_per_share,
    cast(round(coalesce(r.shares_held_prev_day * d.cash_per_adjusted_share, 0), 2) as decimal(16, 2)) as dividend_cash
from with_prev r
left join {{ ref('fct_daily_prices') }} p
    on p.ticker = r.ticker and p.trade_date = r.position_date
left join dividends d
    on d.ticker = r.ticker and d.ex_date = r.position_date
left join {{ ref('dim_account') }} a
    on a.account_id = r.account_id
where r.shares_held > 0 or r.shares_held_prev_day > 0       -- holding, or the day it was sold out
