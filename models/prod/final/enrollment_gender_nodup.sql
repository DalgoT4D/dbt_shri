
{{ config(
  materialized='table'
) }}

with cte as ({{ dbt_utils.deduplicate(
    relation= ref('enrollment_aggregated'),
    partition_by='userid',
    order_by='date_enrollment'
   )
}})


select yob,
       gender,
       userid,
       _submitted_by,
       facility,
       date_enrollment,
       age_years ,
       age_cat
from cte