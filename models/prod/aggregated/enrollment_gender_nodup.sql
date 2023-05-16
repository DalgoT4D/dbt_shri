
{{ config(
  materialized='table',
  schema='aggregated'
) }}


with cte as ({{ remove_duplicates('enrollment_production', 'userid', 'date_enrollment') }})


select yob,
       gender,
       userid,
       _submitted_by,
       facility,
       date_enrollment,
       age_years ,
       age_cat
from cte