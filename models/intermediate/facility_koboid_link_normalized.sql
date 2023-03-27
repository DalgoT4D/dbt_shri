{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}


{{
    flatten_json(
        model_name = source('source_shri_surveys', 'facility_koboid_link'),
        json_column = '_airbyte_data',
        json_fields_to_retain =  ['_id', 'kobo_username', 'facilityname', '_submission_time', '_status', 'endtime', 'starttime', '_submitted_by'],
        non_json_column_fields = ['_airbyte_ab_id', '_airbyte_emitted_at']
    )
}}

