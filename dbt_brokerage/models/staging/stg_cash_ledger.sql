-- Staging: one row per deposit or withdrawal (Azure SQL cash ledger, via silver).
select
    cash_txn_id,
    account_id,
    txn_type,
    amount,                              -- always positive; the direction is in txn_type
    effective_date,
    _silver_loaded_at as loaded_at
from {{ source('silver', 'cash_ledger') }}
