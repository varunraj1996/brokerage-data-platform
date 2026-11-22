-- Gate silver → gold and end to end for the cash ledger, per movement type:
--   generator truth (sim) = silver (from Azure SQL) = gold, in row count and total amount.
with truth as (
    select txn_type, count(*) as n, cast(sum(amount) as decimal(18, 2)) as total
    from {{ source('sim', 'cash_movements') }} group by txn_type
),

silver as (
    select txn_type, count(*) as n, cast(sum(amount) as decimal(18, 2)) as total
    from {{ source('silver', 'cash_ledger') }} group by txn_type
),

gold as (
    select movement_type as txn_type, count(*) as n, cast(sum(abs(amount)) as decimal(18, 2)) as total
    from {{ ref('fct_cash_movements') }} where flow_type = 'external' group by movement_type
)

select coalesce(t.txn_type, s.txn_type, g.txn_type) as txn_type,
       t.n as truth_rows, s.n as silver_rows, g.n as gold_rows,
       t.total as truth_total, s.total as silver_total, g.total as gold_total
from truth t
full outer join silver s on s.txn_type = t.txn_type
full outer join gold g on g.txn_type = coalesce(t.txn_type, s.txn_type)
where t.n is null or s.n is null or g.n is null
   or t.n <> s.n or s.n <> g.n
   or t.total <> s.total or s.total <> g.total
