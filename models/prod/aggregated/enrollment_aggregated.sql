-- Selecting specific columns from the 'enrollment' table

SELECT yob, 
       gender, 
       id_num as userid, 
       _submitted_by,
       submission_time, 
       facility_updated AS facility, 
       date_enrollment, 
       age_years, 
       age_cat
FROM {{ref('enrollment')}}