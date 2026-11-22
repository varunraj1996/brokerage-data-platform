-- Every dividend earned in positions is booked exactly once as cash, to the cent.
select positions_total, cash_total
from (
    select (select coalesce(sum(dividend_cash), 0) from {{ ref('fct_daily_positions') }}) as positions_total,
           (select coalesce(sum(amount), 0) from {{ ref('fct_cash_movements') }}
             where movement_type = 'DIVIDEND')                                       as cash_total
)
where positions_total <> cash_total
