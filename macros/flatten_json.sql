{% macro flatten_json(model_name, json_column, json_fields_to_retain, non_json_column_fields) %}
  
{% set survey_methods_query %}

SELECT DISTINCT(jsonb_object_keys({{json_column}})) as column_name
from {{model_name}}
{% endset %}

{% set results = run_query(survey_methods_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

select
{{ non_json_column_fields | join(', ') }},
{% for column_name in json_fields_to_retain %}
{{ json_column }}->>'{{ column_name }}' as {{ column_name | replace('/', '_') | replace('-', '_') }}{% if not loop.last %},{% endif %}
{% endfor %}
from {{model_name}}
{% endmacro %}