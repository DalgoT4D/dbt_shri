{{ config(
  materialized='table',
    schema='intermediate'

) }}

with cte as(  
        select
            numberid as userid,
            coalesce(begin_group_ajqop6jqs_name_timestamp_formatted, 
                     begin_group_kthfwkunp_name_timestamp_formatted, 
                     begin_group_5xw8upowl_name_timestamp_formatted) 
            as datetime_auto_day,
            to_date
            ( coalesce ( 
            begin_group_ajqop6jqs_name_timestamp_formatted, 
            begin_group_kthfwkunp_name_timestamp_formatted, 
            begin_group_5xw8upowl_name_timestamp_formatted), 'YYYY-MM-DD') as date_auto,
            {{ dbt_utils.star(from = ref('use_tracking_normalized'), 
            except=['begin_group_5xw8upowl_name_timestamp', 
                    'begin_group_kthfwkunp_repeat_numberid', 
                    'begin_group_ajqop6jqs_name_timestamp', 
                    'begin_group_5xw8upowl_name_timestamp_formatted', 
                    'begin_group_kthfwkunp_name_timestamp_formatted', 
                    'begin_group_ajqop6jqs_name_timestamp_formatted', 
                    'name_timestamp_formatted', 'meta_instancename', 
                    'begin_group_ajqop6jqs_repeat_numberid', 
                    'name_timestamp', 'repeat_numberid', 
                    'begin_group_5xw8upowl_repeat_numberid', 
                    'begin_group_kthfwkunp_name_timestamp', 
                    'numberid', 
                    'time_and_date', 
                    '_uuid', 
                    '_tags', 
                    '_airbyte_ab_id', 
                    'formhub_uuid', 
                    'endtime', 
                    'starttime', 
                    '_id', 
                    '_xform_id_string', 
                    'meta_instanceid', 
                    '_validation_status', 
                    '_geolocation', 
                    '_version_', 
                    '_status', 
                    'meta_instanceid', 
                    '_attachments', 
                    '_notes', 
                    '__version__', 
                    'meta_rootuuid',
                    'meta_deprecatedid', 
                    '_submission_time']) }}
        from {{ ref('use_tracking_normalized') }} 
)

SELECT
    a.*,
    b.facilityname AS facility,
    c.yob AS yob,
    c.gender AS gender
FROM cte AS a
LEFT JOIN {{ ref('facility_koboid_link_normalized') }} AS b
ON a._submitted_by = b.kobo_username
LEFT JOIN {{ ref('enrollment_normalized') }} AS c
ON a.userid = c.id_num
order by date_auto asc