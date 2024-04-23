{{ config(
  materialized='table'
) }}


SELECT  
_id, 
facilityname as facility, 
date_auto,
time_auto,
null as category,
shift_type,
null as issue,
null as fixed,
null as full_partial,
null as num_hours,
null as shutdown
FROM {{ref('daily_issue_form')}}
WHERE _id not in (select _id from {{ref('daily_issue_dashboard')}})