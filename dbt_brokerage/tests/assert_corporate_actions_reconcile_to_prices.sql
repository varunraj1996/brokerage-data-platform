-- Every dividend and split day in the daily prices becomes exactly one event, and nothing else does.
select action_type, expected_events, found_events
from (
    select 'DIVIDEND' as action_type,
           (select count(*) from {{ ref('fct_daily_prices') }} where dividend_cash > 0) as expected_events,
           (select count(*) from {{ ref('fct_corporate_actions') }} where action_type = 'DIVIDEND') as found_events
    union all
    select 'SPLIT',
           (select count(*) from {{ ref('fct_daily_prices') }} where split_factor <> 1),
           (select count(*) from {{ ref('fct_corporate_actions') }} where action_type = 'SPLIT')
)
where expected_events <> found_events
