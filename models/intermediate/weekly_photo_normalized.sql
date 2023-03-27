{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

{{
    flatten_json(
        model_name = source('source_shri_surveys', 'weekly_photo'),
        json_column = '_airbyte_data',
        json_fields_to_retain =  ['_id', '_status', 'endtime', 'facility', 'starttime','_submitted_by', '_submission_time'],
        non_json_column_fields = ['_airbyte_ab_id', '_airbyte_emitted_at']
    )
}}
