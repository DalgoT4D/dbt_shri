{{ config(
  materialized='table'
) }}


select facilityname as facility, shift_type, 
    TO_TIMESTAMP(timestamp_formatted, 'YYYY-MM-DD HH24:MI:SS') AS datetime_auto_day,
    to_date(timestamp_formatted, 'YYYY-MM-DD') AS date_auto
from {{ref('daily_issue_form')}}