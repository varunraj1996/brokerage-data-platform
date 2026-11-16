-- dim_date · grain: one row per calendar day, covering the data window (2024–2025)
-- is_trading_day is added once price data exists (a day is a trading day if SPY has a price).
with days as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2024-01-01' as date)",
        end_date   = "cast('2026-01-01' as date)"
    ) }}
)
select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    cast(date_day as date)                         as calendar_date,
    year(date_day)                                 as year,
    quarter(date_day)                              as quarter,
    month(date_day)                                as month,
    date_format(date_day, 'MMMM')                  as month_name,
    date_format(date_day, 'EEEE')                  as day_name,
    dayofweek(date_day) not in (1, 7)              as is_weekday     -- Spark: 1 = Sunday, 7 = Saturday
from days