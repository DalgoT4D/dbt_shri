{{ config(
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ]
) }}

-- Creating a CTE that flattens the JSON data from the raw_use_tracking table

({{
    flatten_json(
        model_name = source('source_shri_surveys', 'waterquality'),
        json_column = '_airbyte_data'
    )
}})


