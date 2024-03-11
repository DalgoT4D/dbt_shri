{{ config(
  materialized='table'
) }}

-- This model is to join a table with koboid link normalized
-- Read more about with cte here https://www.postgresql.org/docs/current/queries-with.html
with cte as(  
        select
            numberid as userid,

            coalesce(begin_group_ajqop6jqs_name_timestamp_formatted, 
                     begin_group_kthfwkunp_name_timestamp_formatted, 
                     begin_group_5xw8upowl_name_timestamp_formatted,
                     name_timestamp_formatted) 
            as datetime_auto_day,

            to_date
            ( coalesce ( 
            begin_group_ajqop6jqs_name_timestamp_formatted, 
            begin_group_kthfwkunp_name_timestamp_formatted, 
            begin_group_5xw8upowl_name_timestamp_formatted, 
            name_timestamp_formatted), 'YYYY-MM-DD') as date_auto,

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
                    '_airbyte_raw_id', 
                    'formhub_uuid', 
                    'endtime', 
                    'starttime', 
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
    c.gender AS gender,
    c.date_enrollment
FROM cte AS a
RIGHT JOIN {{ ref('facility_koboid_link_normalized') }} AS b
ON a._submitted_by = b.kobo_username
LEFT JOIN {{ ref('enrollment_gender_nodup') }} AS c
ON trim(a.userid) = trim(c.userid)
order by date_auto asc
