
{{ config(
  materialized='table'
) }}

-- Selecting specific columns from the 'nonregular_use_tracking' table

SELECT who, child_girl_number, child_boy_number, passerby_woman_number, passerby_man_number, date_auto, facility
FROM {{ref('nonregular_use_tracking')}}