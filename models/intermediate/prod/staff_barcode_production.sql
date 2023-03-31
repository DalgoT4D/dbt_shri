
{{ config(
  materialized='table',
    schema='final'
) }}

-- Selecting specific columns from the 'enrollment' table

SELECT userid, name_timestamp_formatted, _submission_time, _submitted_by, facility
FROM {{ref('staff_barcode')}}