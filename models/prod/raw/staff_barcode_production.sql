
{{ config(
  materialized='table'
) }}

-- Selecting specific columns from the 'staff_barcode' table

SELECT userid, 
       nametimestampformatted, 
       datetime_auto_day, 
       date_auto, 
       _submission_time, 
       _submitted_by, 
       facility, 
       yob, 
       gender
FROM {{ref('staff_barcode')}}