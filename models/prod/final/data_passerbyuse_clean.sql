{{ config(
  materialized='table'
) }}

-- Selecting specific columns from the 'nonregular_use_tracking' table

{{ log("Running query to select non-regular use tracking data") }}
SELECT
    _id,
    who, 
    child_girl_number::integer, 
    child_boy_number::integer, 
    passerby_woman_number::integer, 
    passerby_man_number::integer, 
    time_auto,
    date_auto, 
    facility,
    highlow,
    highlow_other
       
FROM {{ ref('nonregular_use_tracking') }}
