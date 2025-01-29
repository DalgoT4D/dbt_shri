{{ config(
  materialized='table'
) }}

WITH cte AS(  
  SELECT
    CAST(maintenance_numberid_in AS TEXT) AS userid,  -- Ensure it's always treated as text
    group_qx5fr96_name_timestamp_formatted AS nametimestampformatted,

    COALESCE(group_qx5fr96_name_timestamp_formatted, 
             begin_group_chvgkvrk8_name_timestamp_formatted) AS datetime_auto_day,

    TO_DATE(COALESCE(group_qx5fr96_name_timestamp_formatted, 
            begin_group_chvgkvrk8_name_timestamp_formatted), 'YYYY-MM-DD') AS date_auto,

    {{ dbt_utils.star(from = ref('staff_barcode_normalized'),
     except=['begin_group_chvgkvrk8_name_timestamp_formatted', 
             'group_qx5fr96_name_timestamp_formatted', 
             '_airbyte_raw_id', 
             'start', 
             'starttime', 
             '_validation_status', 
             'formhub_uuid', 
             'maintenance_numberid_in', 
             '_uuid', 
             '_xform_id_string', 
             '_tags', 
             '_geolocation', 
             '_status', 
             'meta_instanceid', 
             '_attachments', 
             '_notes', 
             'begin_group_chvgkvrk8_name_timestamp', 
             '__version__', 
             'group_qx5fr96_name_timestamp',
             'yob', 
             'gender'
             ])}}
  FROM {{ ref('staff_barcode_normalized') }} 
)

SELECT
    a.*,
    b.facilityname AS facility,
    c.date_enrollment,
    CASE 
        WHEN LENGTH(TRIM(a.userid)) >= 7 THEN TRIM(a.userid)  -- Ensures no extra spaces affect the length check
        WHEN d.position IS NOT NULL THEN d.position  -- Fallback to staffidlink
        ELSE 'Unknown Position'  -- If no position is found
    END AS position
FROM cte AS a
RIGHT JOIN {{ ref('facility_koboid_link_normalized') }} AS b
    ON a._submitted_by = b.kobo_username
LEFT JOIN {{ ref('enrollment_gender_nodup') }} AS c
    ON a.userid = c.userid
LEFT JOIN {{ ref('staffidlink') }} AS d
    ON a.userid = d.userid
