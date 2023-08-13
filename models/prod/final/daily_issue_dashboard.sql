{{ config(
  materialized='table'
) }}




with my_cte as (({{ dbt_utils.union_relations(
    relations=[ref('daily_issue_form_dashboard_aggregate'),ref('daily_issue_form_others_issue')]
) }}))

select 
    _id,
    facility,
    _submission_time,
    date_auto,
    minorissue_type,
    subcategory as category,
    shift_type,
    issue,
    fixed,
    any_issue,
    full_partial,
    num_hours,
    shutdown
from my_cte

