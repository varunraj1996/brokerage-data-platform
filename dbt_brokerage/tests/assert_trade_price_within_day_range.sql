-- Price sanity: every trade price lies inside that stock's real low–high for the day (split-adjusted, half-cent tolerance).
select t.trade_id, t.ticker, t.trade_date, t.price,
       p.low  * p.split_adjusted_close / p.close as day_low,
       p.high * p.split_adjusted_close / p.close as day_high
from {{ ref('fct_trades') }} t
left join {{ ref('fct_daily_prices') }} p
    on p.ticker = t.ticker and p.trade_date = t.trade_date
where p.ticker is null
   or t.price < p.low  * p.split_adjusted_close / p.close - 0.005
   or t.price > p.high * p.split_adjusted_close / p.close + 0.005
