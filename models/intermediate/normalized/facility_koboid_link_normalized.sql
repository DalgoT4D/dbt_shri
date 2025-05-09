{{ config(
   indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ],
    materialized='table'
) }}

WITH flattened AS (
    SELECT * FROM (
        {{
            flatten_json(
                model_name = source('source_shri_surveys', 'facility_koboid_link'),
                json_column = 'data'
            )
        }}
    ) AS derived_flattened  -- avoid raw unaliased subquery
),

deduplicated AS (
    {{ dbt_utils.deduplicate(
        relation='flattened',
        partition_by='_id',
        order_by='_id DESC'
    ) }}
)

SELECT * FROM deduplicated
