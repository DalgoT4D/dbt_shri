{{ config(
  materialized='table'
) }}

SELECT  
_id, 
timestamp_formatted::date as date_auto, 
facilityname as facility, 
shift_type,
false as any_issue
FROM {{ref('daily_issue_form')}}
WHERE (NOT ((minorissue_type LIKE '%1%') AND 
          (minorissue_type LIKE '%2%') AND 
          (minorissue_type LIKE '%3%') AND 
          (minorissue_type LIKE '%4%') AND 
          (minorissue_type LIKE '%5%') AND 
          (minorissue_type LIKE '%6%') AND 
          (minorissue_type LIKE '%7%'))) or minorissue_type is null
