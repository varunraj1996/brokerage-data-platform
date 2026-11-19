-- Gate silver → gold for prices: fct_daily_prices has exactly as many rows as silver.daily_prices.
select silver_rows, gold_rows
from (
    select (select count(*) from {{ source('silver', 'daily_prices') }}) as silver_rows,
           (select count(*) from {{ ref('fct_daily_prices') }})           as gold_rows
)
where silver_rows <> gold_rows
