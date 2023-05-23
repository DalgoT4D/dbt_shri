{{ config(
  materialized='table',
    schema='intermediate'

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
             '_airbyte_ab_id', 
             '_id', 
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
    c.yob AS yob,
    c.gender AS gender,
    c.date_enrollment,
    d.position 
FROM cte AS a
LEFT JOIN {{ ref('facility_koboid_link_normalized') }} AS b
    ON a._submitted_by = b.kobo_username
LEFT JOIN {{ ref('enrollment') }} AS c
    ON a.userid = c.id_num
LEFT JOIN {{ ref ('staffidlink')}} as d
    ON a.userid = d.userid