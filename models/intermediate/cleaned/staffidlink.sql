{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}


select  {{ dbt_utils.star(from= ref('staffidlink_normalized'),
         except=['_xform_id_string', 
                '_tags', 
                '_geolocation', 
                '_status', 
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
from {{ref('staffidlink_normalized')}}