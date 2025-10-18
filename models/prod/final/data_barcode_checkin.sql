{{ config(
  materialized='table'
) }}

-- Staff barcode is using the table from intermediate schema. 

-- Selecting specific columns from the 'staff_barcode' table

SELECT 
    _id,
    userid, 
    facility, 
    TO_TIMESTAMP(datetime_auto_day, 'YYYY-MM-DD HH24:MI:SS')::TIME AS time_auto, 
    date_auto, 
    date_enrollment,
    CASE 
        WHEN position ~ '^[0-9]+$' THEN 'Unknown Position'
        ELSE position
    END AS position
FROM {{ ref('staff_barcode') }}
