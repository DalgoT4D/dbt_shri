{{ config(
   indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ]
) }}



with my_cte as ({{
    flatten_json(
        model_name = source('source_shri_surveys', 'staffidlink'),
        json_column = 'data'
    )
}})


-- Deduplicating the data in the CTE based on the '_id' column

{{ dbt_utils.deduplicate(
    relation='my_cte',
    partition_by='_id',
    order_by='_id desc',
   )
}}
