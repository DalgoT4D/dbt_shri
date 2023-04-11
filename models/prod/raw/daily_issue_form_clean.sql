{{ config(
  materialized='table'
) }}

-- adding the _id here to track each record. 
select _id, facilityname, shift_type, 
 Case
  when minorissue_type is not null then true
  else false
 end as any_issue,
 
 -- changing the stattime to date here
 starttime::date FROM {{ref('daily_issue_form')}}
