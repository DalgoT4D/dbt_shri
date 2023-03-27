{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_id'], 'type': 'hash'}
    ],
    schema='intermediate'
) }}

{{ dbt_utils.deduplicate(
    relation=ref('enrollment_normalized'),
    partition_by='_id',
    order_by='_id desc',
   )
}}