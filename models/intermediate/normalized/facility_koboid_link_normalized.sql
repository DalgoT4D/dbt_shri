{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]
) }}

-- Creating a CTE that flattens the JSON data from the raw_facility_koboid_link table

with my_cte as ({{
    flatten_json(
        model_name = source('source_shri_surveys', 'facility_koboid_link'),
        json_column = '_airbyte_data'
    )
}})

-- Deduplicating the data in the CTE based on the '_id' column

{{ dbt_utils.deduplicate(
    relation='my_cte',
    partition_by='_id',
    order_by='_id desc',
   )
}}

