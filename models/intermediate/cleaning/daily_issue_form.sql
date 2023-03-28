
{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

select
_submitted_by as username,
{{ dbt_utils.star(from= ref('daily_issue_form_normalized'), except=['_submitted_by', '_notes', '_geolocation', '_version_']) }}

from {{ ref('daily_issue_form_normalized') }} 