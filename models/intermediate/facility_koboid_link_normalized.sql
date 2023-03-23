{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}


select
        _airbyte_ab_id,
        _airbyte_emitted_at,
        _airbyte_data ->> 'facilityname' as facility_name,
        _airbyte_data ->> 'kobo_username' as kobo_username,
        _airbyte_data ->> '_submission_time' as _submission_time

from {{ source('source_shri_surveys', 'facility_koboid_link') }}