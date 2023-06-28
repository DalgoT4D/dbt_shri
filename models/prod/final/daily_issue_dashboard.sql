{{ config(
  materialized='table',
  schema='final'
) }}




with my_cte as (({{ dbt_utils.union_relations(
    relations=[ref('daily_issue_form_dashboard'),ref('daily_issue_others')]
) }}))

select 
    _id,
    facility,
    _submitted_by,
    date_auto,
    minorissue_type,
    subcategory,
    shift_type,
    issue,
    fixed,
    any_issue,
    full_partial,
    num_hours,
    shutdown
from my_cte

