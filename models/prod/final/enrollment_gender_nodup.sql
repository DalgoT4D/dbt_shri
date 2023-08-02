
{{ config(
  materialized='table'
) }}

with cte as ({{ get_latest_row('enrollment_aggregated', 'userid', 'submission_time') }})

select formtimestampformatted,
       yob,
       gender,
       userid,
       _submitted_by,
       facility,
       date_enrollment,
       age_years ,
       age_cat
from cte