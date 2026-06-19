{{ config(
   indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ],
    materialized='table'
) }}

{% set survey_methods_query %}
SELECT DISTINCT(jsonb_object_keys(data)) AS column_name
FROM {{ source('source_shri_surveys', 'facility_koboid_link') }}
{% endset %}

{% if execute %}
{% set survey_methods_results = run_query(survey_methods_query) %}
{% set facility_link_keys = survey_methods_results.columns[0].values() %}
{% else %}
{% set facility_link_keys = [] %}
{% endif %}

WITH flattened AS (
    SELECT * FROM (
        {{
            flatten_json(
                model_name = source('source_shri_surveys', 'facility_koboid_link'),
                json_column = 'data'
            )
        }}
    ) AS derived_flattened  -- avoid raw unaliased subquery
),

deduplicated AS (
    {{ dbt_utils.deduplicate(
        relation='flattened',
        partition_by='_id',
        order_by='_id DESC'
    ) }}
)

SELECT
    _airbyte_raw_id,
    {% for column_name in facility_link_keys %}
    {%- set flattened_column_name = column_name
        | replace('begin_group_yeQ4kl9Kt/', '')
        | replace('begin_group_G8GvBDlis/', '')
        | replace('/', '_')
        | replace('-', '_')
        | lower -%}
    {% if flattened_column_name == 'kobo_username' %}
    CASE
        -- Correct the Kobo username typo so Sayal North submissions join downstream models.
        WHEN facilityname = 'Sayal North Patratu'
            AND kobo_username = 'savalnorthratratu'
            THEN 'sayalnorthpatratu'
        ELSE kobo_username
    END AS kobo_username
    {% else %}
    {{ adapter.quote(flattened_column_name) }}
    {% endif %}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM deduplicated
