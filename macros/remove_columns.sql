{% macro remove_columns(query, columns_to_remove) %}
  {% set column_names = query.columns %}
  {% set new_column_names = [] %}

  {% for column_name in column_names %}
    {% if column_name not in columns_to_remove %}
      {% set new_column_names = new_column_names + [column_name] %}
    {% endif %}
  {% endfor %}

  select
    {{ new_column_names | join(', ') }}
  from {{ query.sql }}
{% endmacro %}