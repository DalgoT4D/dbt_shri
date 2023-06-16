-- The provided DBT code snippet performs the following operations:

-- 1. `{{ config(...) }}`: This DBT macro sets the configuration options for the subsequent block of code. 
--     In this case, it specifies that the resulting table should be materialized as a regular table in the 'intermediate' 
--     schema and includes an index on the '_airbyte_ab_id' column.

-- 2. CTE (Common Table Expression) Definition: The code starts with a CTE named `my_cte`. 
--    It uses the `flatten_json` macro to flatten the JSON data from the 'source_shri_surveys' model's 'daily_issue_form' table. 
--    The `flatten_json` macro takes the model name and the JSON column name ('_airbyte_data') as parameters.

-- 3. Deduplication: After the CTE definition, the code uses the `dbt_utils.deduplicate` macro to deduplicate the data in the CTE. 
--    It specifies the CTE name ('my_cte') as the relation to deduplicate, partitions the data by the '_id' column, 
--    and orders the data within each partition in descending order based on '_id'. This ensures that only the most recent records for each '_id' value are retained.



{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

-- Creating a CTE that flattens the JSON data from the raw_daily_issue_form table

with my_cte as ({{
    flatten_json(
        model_name = source('source_shri_surveys', 'daily_issue_form'),
        json_column = '_airbyte_data'
    )
}})

-- Deduplicating the data in the CTE based on the '_id' column

({{ dbt_utils.deduplicate(
    relation='my_cte',
    partition_by='_id',
    order_by='_id desc',
   )
}})
