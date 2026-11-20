-- Staging: one row per trade execution (silver.trades, built by notebook 13).
select
    trade_id,
    event_id,
    account_id,
    ticker,
    side,
    quantity,
    price,
    fees,
    gross_amount,
    cash_impact,
    executed_at,
    trade_date,
    emitted_at,
    arrival_delay_s,
    is_late,
    _silver_loaded_at as loaded_at
from {{ source('silver', 'trades') }}
