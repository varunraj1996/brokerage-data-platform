-- Every ticker should have a price on every trading day.
-- Warning, not error: a company can legitimately miss days (trading halt, late listing).
{{ config(severity = 'warn') }}

with expected as (
    select u.ticker, d.calendar_date as trade_date
    from {{ ref('ref_universe') }} u
    cross join {{ ref('dim_date') }} d
    where d.is_trading_day
)
select e.ticker, e.trade_date
from expected e
left anti join {{ ref('fct_daily_prices') }} f
    on f.ticker = e.ticker and f.trade_date = e.trade_date
