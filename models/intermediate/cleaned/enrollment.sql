{{ config(
   indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ],
    materialized='table'
) }}

-- Creating a CTE that selects and renames columns from the 'enrollment_normalized' table
WITH cte AS (
    SELECT

        form_timestamp_formatted AS formtimestampformatted,

        -- Selecting all columns from 'enrollment_normalized' except for the ones listed
        {{ dbt_utils.star(from= ref('enrollment_normalized'), 
        except=['yob', 
                '_xform_id_string', 
                'form_timestamp_formatted', 
                '_tags',
                '_submission_time', 
                'name_c', 
                'firstname', 
                '_geolocation', 
                '_status', 
                'meta_rootuuid', 
                'meta_instanceid', 
                'meta_deprecatedid', 
                'mothername', 
                'lastname', 
                'identification_c', 
                '_attachments', 
                '_validation_status', 
                '_notes', 
                'form_timestamp', 
                'formhub_uuid', 
                'lastname_c', 
                '__version__', 
                '_uuid', 
                'firstname_c' ]) }},
        
        -- Creating a new column with the date of enrollment from form_timestamp_formatted
        to_date(form_timestamp_formatted, 'YYYY-MM-DD') AS date_enrollment,
        _submission_time::timestamp AS submission_time,

        -- Creating a new column with the age in years from yob
        CASE  
            WHEN yob::integer < 150 THEN yob::integer 
            ELSE  round((extract(YEAR FROM current_date) - yob::integer)::float::numeric, 1)
        END AS age_years,

        yob::integer

    FROM {{ ref('enrollment_normalized') }} 
)

-- Selecting columns from the CTE and joining with the 'facility_koboid_link_normalized' table to get updated facility data
SELECT
    a.*,
    coalesce(b.facilityname, a.facility) AS facility_updated,  -- Coalescing the facility name from both tables

    -- Creating a new column with age categories based on the age in years
    CASE
        WHEN age_years BETWEEN 1 AND 10 THEN '1-10'
        WHEN age_years BETWEEN 11 AND 20 THEN '11-20'
        WHEN age_years BETWEEN 21 AND 30 THEN '21-30'
        WHEN age_years BETWEEN 31 AND 40 THEN '31-40'
        WHEN age_years BETWEEN 41 AND 50 THEN '41-50'
        WHEN age_years BETWEEN 51 AND 60 THEN '51-60'
        WHEN age_years BETWEEN 61 AND 71 THEN '61-71'
        WHEN age_years BETWEEN 71 AND 81 THEN '71-81'
        WHEN age_years BETWEEN 81 AND 91 THEN '81-91'
        ELSE 'Unknown'
    END AS age_cat
FROM cte AS a RIGHT JOIN {{ ref('facility_koboid_link_normalized') }} AS b ON 
    a._submitted_by = b.kobo_username
