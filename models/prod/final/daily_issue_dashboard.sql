{{ config(
  materialized='table'
) }}




with my_cte as (({{ dbt_utils.union_relations(
    relations=[ref('daily_issue_dashboard_prework'),ref('daily_issue_form_others_issue')]
) }}))

select 
    facility,
    date_auto,
    COALESCE(category, '') AS category,
    COALESCE(issue, '') AS issue,
    COALESCE(shutdown, '') AS shutdown,
    COALESCE(full_partial, '') AS full_partial,
    COALESCE(num_hours, '') AS num_hours

from my_cte


