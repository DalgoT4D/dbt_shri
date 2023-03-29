
{{ config(
  materialized='table',
    schema='intermediate'
) }}

-- Selecting specific columns from the 'enrollment' table

SELECT yob, gender, dataid, formtimestampformatted, _submission_time, _submitted_by, facility_updated AS facility, rootuuid, deprecatedID, date_enrollment, age_years, age_cat
FROM {{ref('enrollment')}}