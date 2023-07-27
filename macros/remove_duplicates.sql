{% macro get_latest_row(table_name, unique_column, date_column) %}
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY {{ unique_column }} ORDER BY {{ date_column }} DESC) AS row_num
        FROM {{ ref(table_name) }}
    )
    SELECT *
    FROM cte
    WHERE row_num = 1
{% endmacro %}

-- `SELECT *, ROW_NUMBER() OVER (PARTITION BY {{ unique_column }} ORDER BY {{ date_column }} DESC) AS row_num FROM {{ ref(table_name) }}`
-- In this part, we select all columns from the table identified by `{{ table_name }}`,
-- and we add an additional column named `row_num` using the `ROW_NUMBER()` window function. 
-- The `ROW_NUMBER()` function assigns a unique row number for each row within the partition defined by the 
-- `{{ unique_column }}` and ordered by the `{{ date_column }}` in descending order. 
-- The row with the latest date for each unique value will have `row_num = 1`.

-- 4. `SELECT * FROM cte WHERE row_num = 1`: 
-- The outer query selects all columns from the CTE `cte`, 
-- but it filters the results to include only the rows where `row_num = 1`. 
-- This effectively keeps only the rows with the latest date for each unique 
-- value in the `{{ unique_column }}` column.

