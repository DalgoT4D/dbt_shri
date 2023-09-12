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
    facility,
    date_auto,
    time_auto,
    COALESCE(shift_type, '') AS shift_type,
    COALESCE(category, '') AS category,
    COALESCE(issue, '') AS issue,
    COALESCE(shutdown, '') AS shutdown,
    COALESCE(full_partial, '') AS full_partial,
    COALESCE(num_hours, '') AS num_hours,
    _id::integer
FROM my_cte
