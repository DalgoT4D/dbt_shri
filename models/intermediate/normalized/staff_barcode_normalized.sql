{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}


with my_cte as ({{
    flatten_json(
        model_name = source('source_shri_surveys', 'staff_barcode'),
        json_column = '_airbyte_data'
    )
}})


{{ dbt_utils.deduplicate(
    relation='my_cte',
    partition_by='_id',
    order_by='_id desc',
   )
}}


