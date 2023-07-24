{{ config(
  materialized='table'
) }}


-- To test the data you can run this query

-- SELECT facility, COUNT(*) as count
-- FROM prod.daily_issue_clean
-- GROUP BY facility;


with my_cte as ({{ dbt_utils.union_relations(
    relations=[ref('daily_issue_dashboard'),ref('daily_issue_form_aggregate')]
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