{% macro get_issue_column_mapping(ref_model) %}
    {%- set issue_dict = {} -%}
    {%- if execute -%}
        {%- set input_relation = ref(ref_model) -%}
        {%- set cols_query -%}
            select column_name
            from information_schema.columns
            where table_schema = '{{ input_relation.schema }}'
              and table_name = '{{ input_relation.identifier }}'
              and column_name ilike '%group_%'
        {%- endset -%}
        {%- set results = run_query(cols_query) -%}
        {%- set column_names = results.columns[0].values() -%}
        {# Define the set of suffixes we expect for outage-type columns #}
        {%- set suffixes = ['fullfacility', 'full', 'hours'] -%}

        {# Loop over each column that contains '_group_' #}
        {%- for col in column_names %}
            {%- set parts = col.split('_group_') %}
            {%- if parts | length < 2 %}
                {# skip invalid names #}
                {%- continue %}
            {%- endif %}
            {%- set category = parts[0] %}
            {%- set rest = parts[1] %}
            {%- set metric = none %}
            {%- set issue = none %}
            
            {# First family: outage-style columns which begin with "outage_" #}
            {%- if rest.startswith('outage_') %}
                {%- set inner = rest[('outage_'|length):] %}
                {%- if inner.endswith('_full') %}
                    {%- set issue = inner[: -('_full'|length)] %}
                    {%- set metric = 'full' %}
                {%- elif inner.endswith('_hours') %}
                    {%- set issue = inner[: -('_hours'|length)] %}
                    {%- set metric = 'hours' %}
                {%- elif inner.endswith('_fullfacility') %}
                    {%- set issue = inner[: -('_fullfacility'|length)] %}
                    {%- set metric = 'fullfacility' %}
                {%- else %}
                    {%- set issue = inner %}
                    {%- set metric = 'outage' %}
                {%- endif %}
            {# Second family: stalls and sides #}
            {%- elif rest.startswith('stalls_') %}
                {%- set issue = rest[('stalls_'|length):] %}
                {%- set metric = 'stalls' %}
            {%- elif rest.startswith('sides_') %}
                {%- set issue = rest[('sides_'|length):] %}
                {%- set metric = 'sides' %}
            {# Third family: fixed columns (which do not have "outage" in the name) #}
            {%- elif rest.endswith('_fixed') %}
                {%- set issue = rest[: -('_fixed'|length)] %}
                {%- set metric = 'fixed' %}
            {%- else %}
                {# if nothing matches, skip this column #}
                {%- continue %}
            {%- endif %}
            
            {# Normalize the issue key (e.g. "bulb" becomes "Bulb") #}
            {%- set issue_key = issue | title %}
            {%- if issue_key not in issue_dict %}
                {%- set _ = issue_dict.update({ issue_key: { 'category': category } }) %}
            {%- endif %}
            {%- set _ = issue_dict[issue_key].update({ metric: col }) %}
        {%- endfor %}
    {%- endif %}
    {{ return(issue_dict) }}
{% endmacro %}