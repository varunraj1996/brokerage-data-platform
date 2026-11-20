-- End to end: every trade the generator created (sim.trades) arrives in fct_trades exactly once, with identical
-- values, despite the duplicates, late events and shuffling the stream added on the way.
--
-- Only the "settled" part of the stream is compared: trades at least 6 days older than the last day emitted.
-- Late events arrive up to 5 days late, so anything newer may legitimately still be on its way.
with cutoff as (
    select date_sub(max(batch_date), 6) as settled_until from {{ source('audit', 'emit_manifest') }}
),

truth as (
    select t.* from {{ source('sim', 'trades') }} t cross join cutoff c
    where t.trade_date <= c.settled_until
),

gold as (
    select f.* from {{ ref('fct_trades') }} f cross join cutoff c
    where f.trade_date <= c.settled_until
)

select coalesce(t.trade_id, g.trade_id) as trade_id,
       case when g.trade_id is null then 'missing in gold'
            when t.trade_id is null then 'in gold but never generated'
            else 'values differ' end      as problem
from truth t
full outer join gold g on g.trade_id = t.trade_id
where t.trade_id is null
   or g.trade_id is null
   or t.account_id  <> g.account_id
   or t.ticker      <> g.ticker
   or t.side        <> g.side
   or t.quantity    <> g.quantity
   or t.price       <> g.price
   or t.fees        <> g.fees
   or t.executed_at <> g.executed_at
   or t.trade_date  <> g.trade_date
