
{{ config(
  materialized='table',
  schema='final'
) }}

-- Staff barcode is using the table from intermediate schema. 

-- Selecting specific columns from the 'staff_barcode' table

SELECT userid, 
       facility, 
       datetime_auto_day, 
       date_auto, 
       yob, 
       gender,
       date_enrollment,
       position
FROM {{ref('staff_barcode')}}


