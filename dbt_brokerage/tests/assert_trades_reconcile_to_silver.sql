-- Gate silver → gold for trades, per trade date: row count, net shares and net cash must match exactly.
-- Returns one row per date that doesn't match (the test fails if any row comes back).
with silver as (
    select trade_date,
           count(*)                                                        as trades,
           sum(case when side = 'BUY' then quantity else -quantity end)    as net_shares,
           round(sum(cash_impact), 2)                                      as net_cash
    from {{ source('silver', 'trades') }}
    group by trade_date
),

gold as (
    select trade_date,
           count(*)                         as trades,
           sum(signed_quantity)             as net_shares,
           round(sum(cash_impact), 2)       as net_cash
    from {{ ref('fct_trades') }}
    group by trade_date
)

select coalesce(s.trade_date, g.trade_date) as trade_date,
       s.trades as silver_trades, g.trades as gold_trades,
       s.net_shares as silver_net_shares, g.net_shares as gold_net_shares,
       s.net_cash as silver_net_cash, g.net_cash as gold_net_cash
from silver s
full outer join gold g on g.trade_date = s.trade_date
where s.trade_date is null or g.trade_date is null
   or s.trades <> g.trades or s.net_shares <> g.net_shares or s.net_cash <> g.net_cash
