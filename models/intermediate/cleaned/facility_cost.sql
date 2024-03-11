{{ config(
  materialized='table'
) }}


select totals_inr, facility from {{source('source_shri_surveys', 'facility_cost')}}