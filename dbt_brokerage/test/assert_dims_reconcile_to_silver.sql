-- Gate silver → gold: every dimension has exactly as many rows as its silver table.
-- A test passes when it returns no rows.
with counts as (
    select 'advisors' as entity,
           (select count(*) from {{ source('silver', 'advisors') }}) as silver_rows,
           (select count(*) from {{ ref('dim_advisor') }})         as gold_rows
    union all
    select 'clients',
           (select count(*) from {{ source('silver', 'clients') }}),
           (select count(*) from {{ ref('dim_client') }})
    union all
    select 'accounts',
           (select count(*) from {{ source('silver', 'accounts') }}),
           (select count(*) from {{ ref('dim_account') }})
)
select * from counts where silver_rows <> gold_rows
