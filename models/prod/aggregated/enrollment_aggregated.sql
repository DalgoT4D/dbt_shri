-- Selecting specific columns from the 'enrollment' table
{{ config(
  materialized='table'
) }}

SELECT
    formtimestampformatted,
    yob, 
    gender, 
    id_num AS userid, 
    _submitted_by,
    submission_time, 
    facility_updated AS facility, 
    date_enrollment, 
    age_years, 
    age_cat
FROM {{ ref('enrollment') }}
