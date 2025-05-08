{{ config(
   indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ],
    materialized='table'
) }}

select  
    staff_userid as userid,
    staff_position as position,

    {{ dbt_utils.star(from= ref('staffidlink_normalized'),
         except=['_xform_id_string', 
                '_tags', 
                'staff_userid',
                '_geolocation', 
                '_status', 
                'staff_position',
                'meta_instanceid', 
                'meta_instancename', 
                'meta_deprecatedid', 
                '_attachments', 
                '_validation_status', 
                '_notes', 
                'form_timestamp', 
                'formhub_uuid', 
                '__version__', 
                '_uuid',
                '_submission_time',
                'endtime',
                'starttime',
                '_submitted_by' ]) }}

from {{ ref('staffidlink_normalized') }}
