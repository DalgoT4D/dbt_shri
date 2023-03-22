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
        json_column = '_airbyte_data'
    )
}}
