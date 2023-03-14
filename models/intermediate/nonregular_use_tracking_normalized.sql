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
        _airbyte_data ->> '__version__' as version,
        _airbyte_data ->> 'meta/instanceID' as instanceID,
        _airbyte_data ->> 'meta/instanceName' as instanceName,
        _airbyte_data ->> '_xform_id_string' as _xform_id_string,
        _airbyte_data ->> '_status' as _status,
        _airbyte_data ->> '_submission_time' as _submission_time,
        _airbyte_data ->> '_submitted_by' as _submitted_by,
        _airbyte_data ->> 'shift_type' as shift_type,
        _airbyte_data ->> 'being_who/who' as who,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/child_girl_number' as child_girl_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/passerby_woman_number' as passerby_woman_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/child_boy_number' as child_boy_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/passerby_man_number' as passerby_man_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/child_girl_number' as v2child_girl_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/passerby_woman_number' as v2passerby_woman_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/child_boy_number' as v2child_boy_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/passerby_man_number' as v2passerby_man_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/child_girl_number' as v3child_girl_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/passerby_woman_number' as v3passerby_woman_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/child_boy_number' as v3child_boy_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/passerby_man_number' as v3passerby_man_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/child_girl_number' as v4child_girl_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/passerby_woman_number' as v4passerby_woman_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/child_boy_number' as v4child_boy_number,
        _airbyte_data ->> 'begin_group_B0SPfk4Xt/passerby_man_number' as v4passerby_man_number


from {{ source('source_shri_surveys', 'nonregular_use_tracking') }}