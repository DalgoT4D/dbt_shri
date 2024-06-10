{{ config(
  materialized='table'
) }}


-- To test the data you can run this query

-- SELECT facility, COUNT(*) as count
-- FROM prod.daily_issue_clean
-- GROUP BY facility;


with my_cte as ({{ dbt_utils.union_relations(
    relations=[ref('daily_issue_dashboard'),ref('daily_issue_clean_prework')]
) }})


SELECT 
    _id::integer,
    COALESCE(full_facility, '') as full_facility,
    COALESCE(stalls, '') as stalls,
    COALESCE(sides, ''), as sides,
    COALESCE(facility, '') as facility,
    COALESCE(shift_type, '') as shift_type,
    COALESCE(category, '') as category,
    date_auto,                             -- Not replacing NULLs here
    time_auto,                             -- Not replacing NULLs here
    COALESCE(issue, '') as issue,
    COALESCE(shutdown, '') as shutdown,
    COALESCE(full_partial, '') as full_partial,
    COALESCE(num_hours::text, '') as num_hours

FROM my_cte
