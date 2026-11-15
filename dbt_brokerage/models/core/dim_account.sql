-- dim_account · grain: one row per account
select
    {{ dbt_utils.generate_surrogate_key(['acc.account_id']) }} as account_sk,
    acc.account_id,
    acc.account_number,
    acc.account_type,
    acc.account_status,
    acc.account_status = 'Open' as is_open,
    acc.opened_date,
    acc.closed_date,
    acc.client_id,
    cl.advisor_id
from {{ ref('stg_accounts') }} acc
left join {{ ref('stg_clients') }} cl using (client_id)
