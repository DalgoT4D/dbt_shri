{{ config(
  materialized='table'
) }}

with my_cte as ({{ dbt_utils.union_relations(
    relations=[ref('daily_issue_dashboard'),ref('daily_issue_clean_prework')]
) }})

select 
    _id::integer,
    COALESCE(full_facility, '') as full_facility,
    COALESCE(stalls, '') as stalls,
    COALESCE(sides, '') as sides,
    COALESCE(facility, '') as facility,
    COALESCE(shift_type, '') as shift_type,
    COALESCE(category, '') as category,
    date_auto,                             -- Not replacing NULLs here
    time_auto,                             -- Not replacing NULLs here
    COALESCE(issue, '') as issue,
    COALESCE(shutdown, '') as shutdown,
    COALESCE(full_partial, '') as full_partial,
    day_shutdown,
    shift_shutdown,
    COALESCE(num_hours::text, '') as num_hours

from my_cte
