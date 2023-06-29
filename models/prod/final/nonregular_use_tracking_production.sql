{{ config(
  materialized='table',
  schema='final'
) }}

-- Selecting specific columns from the 'nonregular_use_tracking' table

{{ log("Running query to select non-regular use tracking data") }}
SELECT _id,
       starttime, 
       who, 
       child_girl_number::integer, 
       child_boy_number::integer, 
       passerby_woman_number::integer, 
       passerby_man_number::integer, 
       date_auto, 
       facility
FROM {{ref('nonregular_use_tracking')}}