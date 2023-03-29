
{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

WITH cte AS (
    SELECT
        meta_deprecatedid as deprecatedID,
        meta_rootuuid as rootuuid,
        form_timestamp_formatted as formtimestampformatted,
        {{ dbt_utils.star(from= ref('enrollment_normalized'), except=['_xform_id_string', 'form_timestamp_formatted', 'id_num', '_tags', 'name_c', 'firstname', '_geolocation', '_status', 'meta_rootuuid', 'meta_instanceid', 'meta_deprecatedid', 'mothername', 'lastname', 'identification_c', '_attachments', '_validation_status', '_notes', 'form_timestamp', 'formhub_uuid', 'lastname_c', '__version__', '_uuid', 'firstname_c']) }},
        to_date(form_timestamp_formatted, 'YYYY-MM-DD') AS date_enrollment,
        ROUND(CAST(EXTRACT(YEAR FROM CURRENT_DATE) - CAST(yob AS integer) AS float)::numeric, 1) AS age_years
    from {{ ref('enrollment_normalized') }} 
)

SELECT
    a.*,
    coalesce(b.facilityname, a.facility) as facility_updated,
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
FROM cte AS a LEFT JOIN {{ ref('facility_koboid_link_normalized') }} AS b on 
a._submitted_by = b.kobo_username