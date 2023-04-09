{% macro join_tables(left_table, right_table, join_type, join_columns) %}

{% if join_type == 'left' %}
SELECT *
FROM {{ left_table }}
LEFT JOIN {{ right_table }}
ON {{ join_columns }}
{% elif join_type == 'right' %}
SELECT *
FROM {{ right_table }}
RIGHT JOIN {{ left_table }}
ON {{ join_columns }}
{% else %}
SELECT 'Invalid join type: Please use left or right join'
{% endif %}

{% endmacro %}
