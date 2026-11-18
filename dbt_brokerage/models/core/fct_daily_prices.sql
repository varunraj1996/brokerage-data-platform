-- fct_daily_prices · grain: one row per security per trading day
-- security_sk is the dim_security version that was valid on the trading day (as-of join on CIK).
with prices as (
    select * from {{ ref('stg_daily_prices') }}
),

securities as (
    select ticker, cik from {{ ref('stg_securities') }}
)

select
    cast(date_format(p.trade_date, 'yyyyMMdd') as int) as date_key,
    d.security_sk,
    p.ticker,
    p.trade_date,
    p.open,
    p.high,
    p.low,
    p.close,
    p.split_factor,
    p.split_adjusted_close,
    p.dividend_cash,
    p.total_return_close,
    p.volume,
    -- day-over-day returns (null on each ticker's first day)
    p.split_adjusted_close
        / lag(p.split_adjusted_close) over (partition by p.ticker order by p.trade_date) - 1  as daily_price_return,
    p.total_return_close
        / lag(p.total_return_close) over (partition by p.ticker order by p.trade_date) - 1    as daily_total_return
from prices p
left join securities s
    on s.ticker = p.ticker
left join {{ ref('dim_security') }} d
    on d.cik = s.cik
   and p.trade_date between d.valid_from and d.valid_to
