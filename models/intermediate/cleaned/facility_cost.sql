{{ config(
  materialized='table'
) }}


select total_quarterly_inr as totals_inr, facility from {{source('source_shri_surveys', 'facility_cost')}}