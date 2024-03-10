{% macro flatten_json(model_name, json_column) %}
  
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
_airbyte_raw_id,
{% for column_name in results_list %}
{{ json_column }}->>'{{ column_name }}' as {{ column_name | replace('begin_group_yeQ4kl9Kt/', '') | replace('begin_group_G8GvBDlis/', '') | replace('/', '_') | replace('-', '_') }}{% if not loop.last %},{% endif %}
{% endfor %}
from {{model_name}}
{% endmacro %}