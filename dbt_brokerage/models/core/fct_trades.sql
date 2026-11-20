{{ config(
    materialized = 'incremental',
    unique_key = 'trade_id',
    incremental_strategy = 'merge',
    on_schema_change = 'fail',
    liquid_clustered_by = ['trade_date', 'account_id']
) }}

-- fct_trades · grain: one row per trade execution
-- Incremental: after the first build, each run only reads silver rows LOADED since the last run.
-- The filter is on load time, not trade_date, so a trade that arrives late (with an old trade_date) is still picked up.
with trades as (
    select * from {{ ref('stg_trades') }}
    {% if is_incremental() %}
    where loaded_at > (select coalesce(max(loaded_at), timestamp'1900-01-01') from {{ this }})
    {% endif %}
),

securities as (
    select ticker, cik from {{ ref('stg_securities') }}
)

select
    t.trade_id,
    cast(date_format(t.trade_date, 'yyyyMMdd') as int)               as date_key,
    a.account_sk,
    d.security_sk,
    t.account_id,
    t.ticker,
    t.side,
    t.quantity,
    case when t.side = 'BUY' then t.quantity else -t.quantity end    as signed_quantity,
    t.price,
    t.gross_amount,
    t.fees,
    t.cash_impact,
    t.executed_at,
    t.trade_date,
    t.is_late,
    t.event_id,
    t.loaded_at
from trades t
left join {{ ref('dim_account') }} a
    on a.account_id = t.account_id
left join securities s
    on s.ticker = t.ticker
left join {{ ref('dim_security') }} d          -- as-of join: the security version valid on the trade date
    on d.cik = s.cik
   and t.trade_date between d.valid_from and d.valid_to
