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
        _airbyte_data ->> '_id' as id,
        _airbyte_data ->> '_uuid' as uuid,
        _airbyte_data ->> 'start' as starttime,
        _airbyte_data ->> 'maintenance_numberid_in' as maintenance_numberid_in,
        _airbyte_data ->> 'group_qx5fr96/name-timestamp' as nametimestamp,
        _airbyte_data ->> 'group_qx5fr96/name-timestamp-formatted' as nametimestampformatted,
        _airbyte_data ->> '__version__' as version,
        _airbyte_data ->> 'meta/instanceID' as instanceID,
        _airbyte_data ->> '_xform_id_string' as _xform_id_string,
        _airbyte_data ->> '_status' as _status,
        _airbyte_data ->> '_submission_time' as _submission_time,
        _airbyte_data ->> '_submitted_by' as _submitted_by

from {{ source('source_shri_surveys', 'staff_barcode') }}


