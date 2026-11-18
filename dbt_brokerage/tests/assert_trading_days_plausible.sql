-- 2024 and 2025 each had about 250–252 US trading days. Anything far off means missing or extra price dates.
select year, count(*) as trading_days
from {{ ref('dim_date') }}
where is_trading_day
group by year
having count(*) not between 245 and 255
