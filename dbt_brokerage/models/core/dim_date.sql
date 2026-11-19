-- dim_date · grain: one row per calendar day, covering the data window (2024–2025)
-- A day is a trading day when the market was open, i.e. SPY has a price that day.
-- This handles weekends and market holidays automatically.
with days as (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2024-01-01' as date)",
        end_date   = "cast('2026-01-01' as date)"
    ) }}
),

market_open as (
    select distinct trade_date
    from {{ ref('stg_daily_prices') }}
    where ticker = 'SPY'
)

select
    cast(date_format(d.date_day, 'yyyyMMdd') as int) as date_key,
    cast(d.date_day as date)                         as calendar_date,
    year(d.date_day)                                 as year,
    quarter(d.date_day)                              as quarter,
    month(d.date_day)                                as month,
    date_format(d.date_day, 'MMMM')                  as month_name,
    date_format(d.date_day, 'EEEE')                  as day_name,
    dayofweek(d.date_day) not in (1, 7)              as is_weekday,      -- Spark: 1 = Sunday, 7 = Saturday
    m.trade_date is not null                         as is_trading_day
from days d
left join market_open m on m.trade_date = cast(d.date_day as date)
