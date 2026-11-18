-- Staging: one row per ticker per trading day.
select
    ticker,
    trade_date,
    open,
    high,
    low,
    close,                                   -- as traded that day
    split_factor,                            -- 10 on the day of a 10-for-1 split, else 1
    split_adjusted_close,                    -- in today's share units: used to value positions
    dividend_cash,                           -- per share, on the ex-dividend date
    adj_close as total_return_close,         -- split- and dividend-adjusted: used for benchmark returns
    volume,
    _silver_loaded_at as loaded_at
from {{ source('silver', 'daily_prices') }}
