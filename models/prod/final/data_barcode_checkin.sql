
{{ config(
  materialized='table'
) }}

-- Staff barcode is using the table from intermediate schema. 

-- Selecting specific columns from the 'staff_barcode' table

SELECT 
       _id,
       userid, 
       facility, 
       TO_TIMESTAMP(datetime_auto_day, 'YYYY-MM-DD HH24:MI:SS')::TIME as time_auto, 
       date_auto, 
       yob, 
       gender,
       date_enrollment,
       position
FROM {{ref('staff_barcode')}}


