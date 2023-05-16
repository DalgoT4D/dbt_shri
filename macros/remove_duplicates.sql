{% macro remove_duplicates(table_name, unique_column, date_column) %}
    WITH cte AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY {{ unique_column }} ORDER BY {{ date_column }} DESC) AS row_num
        FROM {{ ref(table_name) }}
    )
    SELECT *
    FROM cte
    WHERE ({{ unique_column }}, {{ date_column }}) NOT IN (
        SELECT {{ unique_column }}, MAX({{ date_column }})
        FROM cte
        WHERE row_num > 1
        GROUP BY {{ unique_column }}
    )
{% endmacro %}