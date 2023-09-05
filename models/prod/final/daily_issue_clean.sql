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


select 
    facility,
    date_auto,
    time_auto,
    shift_type,
    category,
    issue,
    shutdown,
    full_partial,
    num_hours,
    _id

from my_cte