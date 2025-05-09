{{ config(
  materialized='table'
) }}


with my_cte as (({{ dbt_utils.union_relations(
    relations=[ref('daily_issue_dashboard_prework'),ref('daily_issue_form_others_issue')]
) }})
)

select 
    COALESCE(_id::text, '') as _id,
    COALESCE(facility, '') as facility,
    date_auto,
    time_auto,
    COALESCE(category, '') as category,
    COALESCE(shift_type, '') as shift_type,
    COALESCE(issue, '') as issue,
    COALESCE(fixed, '') as fixed,
    COALESCE(full_partial, '') as full_partial,
    COALESCE(num_hours::text, '') as num_hours,
    COALESCE(shutdown, '') as shutdown

from my_cte
