{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

       {%- if target.name != "prod" -%}
            {% if node.fqn[1:-1]|length == 0 %}
                 {{target.name}}_{{ default_schema }}    
            {% else %}
                {% set prefix = node.fqn[1:-1]|join('_') %}
                 {{target.name}}_{{ prefix | trim }}
            {% endif %}


       {% else %} 
            {% if node.fqn[1:-1]|length == 0 %}
                {{ default_schema }}    
            {% else %}
                {% set prefix = node.fqn[1:-1]|join('_') %}
                {{ prefix | trim }}
            {% endif %}
         {% endif %}
    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
