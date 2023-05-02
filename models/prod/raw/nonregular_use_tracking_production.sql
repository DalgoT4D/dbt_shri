{{ config(
  materialized='table'
) }}

-- Selecting specific columns from the 'nonregular_use_tracking' table

SELECT _id,
       starttime, 
       who, 
       child_girl_number::integer, 
       child_boy_number::integer, 
       passerby_woman_number::integer, 
       passerby_man_number::integer, 
       date_auto, 
       facility, 
       time_auto
FROM {{ref('nonregular_use_tracking')}}