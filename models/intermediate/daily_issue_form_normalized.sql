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
        _airbyte_data ->> '_uuid' as uuid,
        _airbyte_data ->> 'starttime' as starttime,
        _airbyte_data ->> 'endtime' as endtime,
        _airbyte_data ->> 'timestamp' as timestamp,
        _airbyte_data ->> 'timestamp-formatted' as timestampformatted,
        _airbyte_data ->> '__version__' as version,
        _airbyte_data ->> 'meta/instanceID' as instanceID,
        _airbyte_data ->> '_xform_id_string' as _xform_id_string,
        _airbyte_data ->> '_status' as _status,
        _airbyte_data ->> '_submission_time' as _submission_time,
        _airbyte_data ->> '_submitted_by' as _submitted_by,
        _airbyte_data ->> 'shift_type' as shift_type,
        _airbyte_data ->> 'minorissue_type' as minorissue_type


from {{ source('source_shri_surveys', 'daily_issue_form') }}
