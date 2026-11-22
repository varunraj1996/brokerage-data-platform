-- fct_corporate_actions · grain: one row per stock per corporate-action event (dividend or split) on its ex-date
-- Pulled out of fct_daily_prices, where these are columns on every day (1 / 0 on almost all of them).
-- Events are separated from the daily snapshot so positions and cash can react to them (steps 4.4–4.5).
--
-- Units: positions are kept in today's (split-adjusted) share units, so a dividend is also given per
-- split-adjusted share: raw dividend ÷ every split after the ex-date  (= raw × split_adjusted_close / close).
with prices as (
    select * from {{ ref('fct_daily_prices') }}
),

dividends as (
    select
        date_key, security_sk, ticker, trade_date                          as ex_date,
        'DIVIDEND'                                                         as action_type,
        dividend_cash                                                      as cash_per_share,
        round(dividend_cash * split_adjusted_close / close, 6)             as cash_per_adjusted_share,
        cast(null as double)                                               as split_ratio
    from prices
    where dividend_cash > 0
),

splits as (
    select
        date_key, security_sk, ticker, trade_date                          as ex_date,
        'SPLIT'                                                            as action_type,
        cast(null as double)                                               as cash_per_share,
        cast(null as double)                                               as cash_per_adjusted_share,
        split_factor                                                       as split_ratio
    from prices
    where split_factor <> 1
)

select
    concat(ticker, '-', date_format(ex_date, 'yyyyMMdd'), '-', action_type) as action_id,
    *
from (select * from dividends union all select * from splits)
