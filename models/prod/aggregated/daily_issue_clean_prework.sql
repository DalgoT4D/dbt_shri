{{ config(
  materialized='table'
) }}


SELECT  
    _id, 
    facilityname AS facility, 
    date_auto,
    time_auto,
    null AS category,
    shift_type,
    null AS issue,
    null AS fixed,
    null AS full_partial,
    null AS num_hours,
    null AS full_facility,
    null AS stalls,
    null AS sides,
    null AS shutdown
FROM {{ ref('daily_issue_form') }}
WHERE _id NOT IN (SELECT _id FROM {{ ref('daily_issue_dashboard') }})
