{{ config(
  materialized='table'
) }}


SELECT 
    facility,
    SUM(total_use) as total_use,
    COUNT(facility) as facility_count,
    ROUND((SUM(total_use) / COUNT(facility))::numeric / 1000, 1) || 'k' as average_use_per
FROM {{ref('usetracking_dashboard')}}
WHERE 
    (facility = 'Dundibagh' AND date_auto >= '2022-02-19')
    OR (facility = 'Basgoda' AND date_auto >= '2022-02-19')
    OR (facility = 'Gomia' AND date_auto >= '2022-02-19')
    OR (facility = 'Azad nagar' AND date_auto >= '2022-08-23')
    OR (facility = 'North Basgoda' AND date_auto >= '2022-08-23')
    OR (facility = 'Peterbaar' AND date_auto >= '2023-01-13')
    OR (facility = 'Jaridih CSR' AND date_auto >= '2023-07-01')
    OR (facility = 'Jaridih SBM' AND date_auto >= '2023-07-01')
    OR (facility = 'Kasmar' AND date_auto >= '2023-07-01')
    OR (facility = 'Vurahi' AND date_auto >= '2023-07-01')
GROUP BY facility
