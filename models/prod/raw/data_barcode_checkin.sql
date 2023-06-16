
{{ config(
  materialized='table'
) }}

-- Staff barcode is using the table from intermediate schema. 

-- Selecting specific columns from the 'staff_barcode' table

SELECT userid, 
       nametimestampformatted, 
       datetime_auto_day, 
       date_auto, 
       date_enrollment,
       _submission_time, 
       _submitted_by, 
       facility, 
       yob, 
       gender,
       position
FROM {{ref('staff_barcode')}}


