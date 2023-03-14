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
        _airbyte_data ->> 'formhub/uuid' as uuid,
        _airbyte_data ->> 'starttime' as starttime,
        _airbyte_data ->> 'endtime' as endtime,
        _airbyte_data ->> '__version__' as version,
        _airbyte_data ->> 'meta/instanceID' as instanceID,
        _airbyte_data ->> 'meta/instanceName' as instanceName,
        _airbyte_data ->> '_xform_id_string' as _xform_id_string,
        _airbyte_data ->> '_status' as _status,
        _airbyte_data ->> '_submission_time' as _submission_time,
        _airbyte_data ->> '_submitted_by' as _submitted_by,
        _airbyte_data ->> 'begin_group_ajqoP6JQS/name-timestamp' as v3nametimestamp,
        _airbyte_data ->> 'begin_group_ajqoP6JQS/name-timestamp-formatted' as v3nametimestampformatted,
        _airbyte_data ->> 'begin_group_ajqoP6JQS/repeat_numberid' as v3repeat_numberid,
        _airbyte_data ->> 'begin_group_KtHfWkunP/name-timestamp' as v2nametimestamp,
        _airbyte_data ->> 'begin_group_KtHfWkunP/name-timestamp-formatted' as v2nametimestampformatted,
        _airbyte_data ->> 'begin_group_KtHfWkunP/repeat_numberid' as v2repeat_numberid,
        _airbyte_data ->> 'begin_group_5Xw8upowl/name-timestamp' as v4nametimestamp,
        _airbyte_data ->> 'begin_group_5Xw8upowl/name-timestamp-formatted' as v4nametimestampformatted,
        _airbyte_data ->> 'begin_group_5Xw8upowl/repeat_numberid' as v4repeat_numberid,
        _airbyte_data ->> 'numberid' as numberid,
        _airbyte_data ->> '_uuid' as _uuid

from {{ source('source_shri_surveys', 'use_tracking') }}