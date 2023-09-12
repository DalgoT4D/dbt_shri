{{ config(
  materialized='table'
) }}




with my_cte as (({{ dbt_utils.union_relations(
    relations=[ref('daily_issue_dashboard_prework'),ref('daily_issue_form_others_issue')]
) }}))

select 
    _id,
    facility,
    date_auto,
    time_auto,
    category,
    shift_type,
    issue,
    fixed,
    full_partial,
    num_hours,
    shutdown

from my_cte