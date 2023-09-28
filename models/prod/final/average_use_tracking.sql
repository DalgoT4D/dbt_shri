{{ config(
  materialized='table'
) }}


SELECT 
    facility,
    SUM(total_use) as total_use,
    COUNT(facility) as facility_count,
    ROUND((SUM(total_use) / COUNT(facility))::numeric / 1000, 1) || 'k' as average_use_per
FROM {{ref('usetracking_dashboard')}}
GROUP BY facility