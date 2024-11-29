{{ config(
  materialized='table'
) }}

with cte as(  
  select
    maintenance_numberid_in as userid,

    group_qx5fr96_name_timestamp_formatted as nametimestampformatted,

    coalesce(group_qx5fr96_name_timestamp_formatted, 
             begin_group_chvgkvrk8_name_timestamp_formatted) as datetime_auto_day,

    to_date(coalesce(group_qx5fr96_name_timestamp_formatted, 
            begin_group_chvgkvrk8_name_timestamp_formatted), 'YYYY-MM-DD') as date_auto,

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
             'group_qx5fr96_name_timestamp'])}}
  from {{ ref('staff_barcode_normalized') }} 
)

SELECT
    a.*,
    b.facilityname AS facility,
    c.date_enrollment,
    CASE 
        WHEN LENGTH(a.userid) >= 7 THEN a.userid  
        ELSE d.position                         
    END AS position
FROM cte AS a
RIGHT JOIN {{ ref('facility_koboid_link_normalized') }} AS b
    ON a._submitted_by = b.kobo_username
LEFT JOIN {{ ref('enrollment_gender_nodup') }} AS c
    ON a.userid = c.userid
LEFT JOIN {{ ref ('staffidlink')}} as d
    ON a.userid = d.userid