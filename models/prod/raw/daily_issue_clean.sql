{{ config(
  materialized='table'
) }}


with my_cte as ({{ dbt_utils.union_relations(
    relations=[ref('daily_issue_form_dashboard'),ref('daily_issue_form_aggregate')]
) }})


select 
    _id,
    facility,
    issue,
    shift_type,
    fixed,
    any_issue,
    full_partial,
    num_hours,
    shutdown,
    date_auto

from my_cte