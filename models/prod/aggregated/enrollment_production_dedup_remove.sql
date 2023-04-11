
{{ config(
  materialized='table'
) }}



({{ dbt_utils.deduplicate(
    relation=ref('enrollment_production'),
    partition_by='userid',
    order_by='_submitted_by desc',
   )
}})
