{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}


{{
    flatten_json(
        model_name = source('source_shri_surveys', 'use_tracking'),
        json_column = '_airbyte_data',
        json_fields_to_retain =  ['_id', '_status', 'endtime', 'starttime', '_submitted_by', '_submission_time', 'begin_group_KtHfWkunP/name-timestamp', 'begin_group_KtHfWkunP/repeat_numberid', 'begin_group_KtHfWkunP/name-timestamp-formatted'],
        non_json_column_fields = ['_airbyte_ab_id', '_airbyte_emitted_at']
    )
}}