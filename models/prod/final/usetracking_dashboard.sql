{{ config(
  materialized='table'
) }}

select * from {{ref('usetracking_dashboard_aggregated')}} where date_auto is not null