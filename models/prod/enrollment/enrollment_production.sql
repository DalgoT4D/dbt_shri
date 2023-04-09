
{{ config(
  materialized='table'
) }}

-- Selecting specific columns from the 'enrollment' table

SELECT yob, gender, dataid, formtimestampformatted, _submission_time, _submitted_by, facility_updated AS facility, date_enrollment, age_years, age_cat
FROM {{ref('enrollment')}}