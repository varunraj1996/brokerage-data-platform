-- fct_cash_movements · grain: one row per movement of cash in or out of an account's cash balance
--
--   flow_type = 'external'  money crossing the account boundary: DEPOSIT, WITHDRAWAL (from the cash ledger)
--   flow_type = 'internal'  money moving between cash and holdings: BUY, SELL (gross), FEE (from fct_trades)
--   flow_type = 'internal'  DIVIDEND: shares held the day before the ex-date × dividend (from fct_daily_positions)
--
-- amount is signed from the account's point of view: + cash in, − cash out.
-- Returns need this split: external flows are removed from performance, internal ones are part of it.

with ledger as (
    select
        concat('C', cast(cash_txn_id as string))        as movement_id,
        account_id,
        effective_date                                  as movement_date,
        txn_type                                        as movement_type,
        'external'                                      as flow_type,
        cast(null as string)                            as ticker,
        cast(null as bigint)                            as trade_id,
        cash_txn_id,
        case when txn_type = 'DEPOSIT' then amount else -amount end  as amount
    from {{ ref('stg_cash_ledger') }}
),

trade_cash as (
    select
        concat('T', cast(trade_id as string))           as movement_id,
        account_id,
        trade_date                                      as movement_date,
        side                                            as movement_type,
        'internal'                                      as flow_type,
        ticker,
        trade_id,
        cast(null as bigint)                            as cash_txn_id,
        case when side = 'BUY' then -gross_amount else gross_amount end  as amount
    from {{ ref('fct_trades') }}
),

fees as (
    select
        concat('F', cast(trade_id as string))           as movement_id,
        account_id,
        trade_date                                      as movement_date,
        'FEE'                                           as movement_type,
        'internal'                                      as flow_type,
        ticker,
        trade_id,
        cast(null as bigint)                            as cash_txn_id,
        -fees                                           as amount
    from {{ ref('fct_trades') }}
    where fees > 0
),

dividends as (
    select
        concat('D', cast(account_id as string), '-', ticker, '-', date_format(position_date, 'yyyyMMdd')) as movement_id,
        account_id,
        position_date                                   as movement_date,
        'DIVIDEND'                                      as movement_type,
        'internal'                                      as flow_type,
        ticker,
        cast(null as bigint)                            as trade_id,
        cast(null as bigint)                            as cash_txn_id,
        dividend_cash                                   as amount
    from {{ ref('fct_daily_positions') }}
    where dividend_cash > 0
),

movements as (
    select * from ledger
    union all select * from trade_cash
    union all select * from fees
    union all select * from dividends
)

select
    m.movement_id,
    cast(date_format(m.movement_date, 'yyyyMMdd') as int)  as date_key,
    a.account_sk,
    m.account_id,
    m.movement_date,
    m.movement_type,
    m.flow_type,
    m.ticker,
    m.trade_id,
    m.cash_txn_id,
    cast(m.amount as decimal(16, 2))                       as amount
from movements m
left join {{ ref('dim_account') }} a
    on a.account_id = m.account_id
