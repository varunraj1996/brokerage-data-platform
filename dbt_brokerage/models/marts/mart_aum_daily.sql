{{ config(materialized = 'table') }}

-- mart_aum_daily · grain: one row per trading day × advisor × client segment
-- AUM (assets under management) = the total value of every account: holdings at market value + cash.
-- Also splits each day's change in AUM into what clients did (net new assets) and what markets did (market effect):
--     AUM_today − AUM_yesterday = net_new_assets + market_effect
select
    v.value_date,
    v.date_key,
    c.advisor_id,
    c.advisor_name,
    c.advisor_team,
    c.segment,
    count(*)                                              as accounts,
    sum(case when v.total_value > 0 then 1 else 0 end)    as funded_accounts,
    sum(v.market_value)                                   as market_value,
    sum(v.cash_balance)                                   as cash,
    sum(v.total_value)                                    as aum,
    sum(v.external_flow)                                  as net_new_assets,
    sum(v.daily_pnl)                                      as market_effect,
    sum(v.dividends)                                      as dividends,
    sum(v.fees)                                           as fees
from {{ ref('fct_daily_account_value') }} v
join {{ ref('dim_account') }} a using (account_id)
join {{ ref('dim_client') }}  c on c.client_id = a.client_id
group by v.value_date, v.date_key, c.advisor_id, c.advisor_name, c.advisor_team, c.segment
