{{ config(
  materialized='table'
) }}

with cte as (SELECT 
    -- passerby issue
    -- renaming a column
    being_who_who as who, 
    -- coalescing required columns from similar fields
    coalesce(begin_group_lky5gqsbu_child_girl_number, begin_group_aypvm5n7n_child_girl_number, 
        begin_group_b0spfk4xt_child_girl_number) as child_girl_number,
    coalesce(begin_group_aypvm5n7n_passerby_women_number, 
        begin_group_b0spfk4xt_passerby_woman_number) as passerby_woman_number,
    coalesce(begin_group_b0spfk4xt_passerby_man_number, 
        begin_group_aypvm5n7n_passerby_man_number) as passerby_man_number,
    coalesce(begin_group_b0spfk4xt_child_boy_number, begin_group_nrhrybak5_child_boy_number, 
        begin_group_aypvm5n7n_child_boy_number) as child_boy_number,

    -- creating a new column date_auto from _submission_time and convert to date format
    to_date(starttime, 'YYYY-MM-DD') AS date_auto,
    SUBSTRING(starttime FROM 'T(\d{2}:\d{2}:\d{2})')::time AS time_auto,

    -- selecting all columns from nonregular_use_tracking_normalized except the ones listed
    {{ dbt_utils.star(from = ref('nonregular_use_tracking_normalized'), 
    except=['being_who_who', 
             '_version_', 
             '_submission_time', 
             'begin_group_aypvm5n7n_child_boy_number', 
             'begin_group_nrhrybak5_child_boy_number', 
             'begin_group_b0spfk4xt_child_boy_number', 
             'begin_group_aypvm5n7n_passerby_man_number', 
             'begin_group_b0spfk4xt_passerby_man_number', 
             'begin_group_b0spfk4xt_child_girl_number', 
             'begin_group_b0spfk4xt_passerby_woman_number', 
             'begin_group_aypvm5n7n_passerby_women_number', 
             'begin_group_aypvm5n7n_child_girl_number', 
             'begin_group_lky5gqsbu_child_girl_number', 
             '_airbyte_ab_id', 
             '_xform_id_string', 
             'endtime', 
             '_validation_status', 
             'formhub_uuid', 
             'meta_instancename', 
             '_uuid', 
             '_tags', 
             '_geolocation', 
             '_version', 
             '_status', 
             'meta_instanceid', 
             '_attachments', 
             '_notes',
             'facility', 
             '__version__', ])}}
    from {{ ref('nonregular_use_tracking_normalized') }} )


SELECT 
    a.*,
    b.facilityname as facility
from cte AS a
RIGHT JOIN {{ ref('facility_koboid_link_normalized') }} AS b
    ON a._submitted_by = b.kobo_username
    
