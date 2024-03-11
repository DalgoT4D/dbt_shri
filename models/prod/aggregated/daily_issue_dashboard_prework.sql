{{ config(
  materialized='table'
) }}


SELECT 
       _id,
       facility,
       date_auto,
       time_auto,
       shift_type,
       minorissue_type,
       category,
       issue,
       CASE full_part 
          WHEN '1' THEN 'part day'
          WHEN '2' THEN 'full day'
        END as full_partial,
       num_hours,
       CASE
          WHEN outage IS NULL THEN 'no'
          WHEN outage = '0' THEN 'no'
          WHEN outage = '1' THEN 'yes'
       ELSE outage
       END as shutdown
FROM {{ref('daily_issue_form_aggregate')}}
where fixed is not null
order by date_auto