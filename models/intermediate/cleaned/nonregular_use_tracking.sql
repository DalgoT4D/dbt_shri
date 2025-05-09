{{ config(
  materialized='table'
) }}


with cte as (
    select 
        -- passerby issue
        -- renaming a column
        being_who_who as who, 
        -- coalescing required columns from similar fields
        coalesce(
            begin_group_lky5gqsbu_child_girl_number, begin_group_aypvm5n7n_child_girl_number, 
            begin_group_b0spfk4xt_child_girl_number
        ) as child_girl_number,
        coalesce(
            begin_group_aypvm5n7n_passerby_women_number, 
            begin_group_b0spfk4xt_passerby_woman_number
        ) as passerby_woman_number,
        coalesce(
            begin_group_b0spfk4xt_passerby_man_number, 
            begin_group_aypvm5n7n_passerby_man_number
        ) as passerby_man_number,
        coalesce(
            begin_group_b0spfk4xt_child_boy_number, begin_group_nrhrybak5_child_boy_number, 
            begin_group_aypvm5n7n_child_boy_number
        ) as child_boy_number,

        -- creating a new column date_auto from _submission_time and convert to date format
        coalesce(
            to_date(being_who_date, 'YYYY-MM-DD'), 
            to_date(starttime, 'YYYY-MM-DD')   
        ) as date_auto,
        substring(starttime from 'T(\d{2}:\d{2}:\d{2})')::time as time_auto,
        begin_group_aypvm5n7n_highlow as highlow,
        begin_group_aypvm5n7n_highlow_other as highlow_other,

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
             '_airbyte_raw_id', 
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
             '__version__', ]) }}
    from {{ ref('nonregular_use_tracking_normalized') }} 
)

select 
    a.*,
    b.facilityname as facility
from cte as a
right join {{ ref('facility_koboid_link_normalized') }} as b
    on a._submitted_by = b.kobo_username    
