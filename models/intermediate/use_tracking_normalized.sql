{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}


select
        _airbyte_ab_id,
        _airbyte_emitted_at

from {{ source('source_shri_surveys', 'use_tracking') }}