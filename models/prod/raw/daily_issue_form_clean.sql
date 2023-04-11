{{ config(
  materialized='table'
) }}


select facilityname, shift_type, 
 Case
  when minorissue_type is not null then true
  else false
 end as any_issue,
 
 starttime::date FROM {{ref('daily_issue_form')}}
