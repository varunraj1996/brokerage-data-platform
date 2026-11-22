{{ config(
    materialized = 'table',
    liquid_clustered_by = ['value_date', 'account_id']
) }}

-- fct_daily_account_value · grain: one row per account per trading day, from its first deposit until it closes
-- A periodic snapshot (semi-additive like positions: sum across accounts on one day, never across days).
--
--   market_value   = Σ positions × close                       (fct_daily_positions)
--   cash_balance   = running total of every cash movement      (fct_cash_movements)
--   total_value  V = market_value + cash_balance
--   external_flow F = deposits − withdrawals that day          (money the client moved, not performance)
--   daily_pnl      = V_today − V_yesterday − F                 (what the market did to the account)
with trading_days as (
    select calendar_date as value_date, date_key
    from {{ ref('dim_date') }}
    where is_trading_day
),

cash_daily as (
    select account_id, movement_date as value_date,
           sum(amount)                                                         as net_cash_flow,
           sum(case when flow_type = 'external' then amount else 0 end)        as external_flow,
           sum(case when movement_type = 'DIVIDEND' then amount else 0 end)    as dividends,
           sum(case when movement_type = 'FEE' then -amount else 0 end)        as fees
    from {{ ref('fct_cash_movements') }}
    group by account_id, movement_date
),

market_daily as (
    select account_id, position_date as value_date,
           sum(market_value)                                   as market_value,
           count(case when shares_held > 0 then 1 end)         as holdings
    from {{ ref('fct_daily_positions') }}
    group by account_id, position_date
),

accounts as (
    select a.account_id, a.account_sk, a.closed_date, min(c.value_date) as first_date
    from {{ ref('dim_account') }} a
    join cash_daily c using (account_id)
    group by a.account_id, a.account_sk, a.closed_date
),

spine as (
    select a.account_id, a.account_sk, t.value_date, t.date_key
    from accounts a
    join trading_days t
      on t.value_date >= a.first_date
     and (a.closed_date is null or t.value_date <= a.closed_date)
),

valued as (
    select s.*,
           coalesce(m.market_value, 0)                                        as market_value,
           coalesce(m.holdings, 0)                                            as holdings,
           coalesce(c.external_flow, 0)                                       as external_flow,
           coalesce(c.dividends, 0)                                           as dividends,
           coalesce(c.fees, 0)                                                as fees,
           sum(coalesce(c.net_cash_flow, 0)) over (
               partition by s.account_id order by s.value_date
               rows between unbounded preceding and current row)              as cash_balance
    from spine s
    left join market_daily m on m.account_id = s.account_id and m.value_date = s.value_date
    left join cash_daily   c on c.account_id = s.account_id and c.value_date = s.value_date
),

with_total as (
    select *, market_value + cash_balance as total_value
    from valued
)

select
    date_key,
    account_sk,
    account_id,
    value_date,
    cast(market_value  as decimal(18, 2))                                       as market_value,
    cast(cash_balance  as decimal(18, 2))                                       as cash_balance,
    cast(total_value   as decimal(18, 2))                                       as total_value,
    holdings,
    cast(external_flow as decimal(18, 2))                                       as external_flow,
    cast(dividends     as decimal(18, 2))                                       as dividends,
    cast(fees          as decimal(18, 2))                                       as fees,
    cast(total_value
         - lag(total_value, 1, 0) over (partition by account_id order by value_date)
         - external_flow as decimal(18, 2))                                     as daily_pnl
from with_total
