{{ config(
  materialized='table'
) }}


WITH 
mycte AS (
    SELECT 
        facility,
        date_auto
    FROM prod_final.usetracking_dashboard
    WHERE 
        (facility = 'Dundibagh' AND date_auto >= '2022-02-19')
        OR (facility = 'Basgoda' AND date_auto >= '2022-02-19')
        OR (facility = 'Gomia' AND date_auto >= '2022-02-19')
        OR (facility = 'Azad Nagar' AND date_auto >= '2022-08-23')
        OR (facility = 'North Basgoda' AND date_auto >= '2022-08-23')
        OR (facility = 'Peterbaar' AND date_auto >= '2023-01-13')
        OR (facility = 'Jaridih CSR' AND date_auto >= '2023-07-01')
        OR (facility = 'Jaridih SBM' AND date_auto >= '2023-07-01')
        OR (facility = 'Kasmar' AND date_auto >= '2023-07-01')
        OR (facility = 'Vurahi' AND date_auto >= '2023-07-01')
    GROUP BY facility, date_auto
),

FacilityDates AS (
    SELECT 
        facility,
        MIN(date_auto) as min_date,
        MAX(date_auto) as max_date
    FROM mycte
    GROUP BY facility
),

FilteredData AS (
    SELECT 
        u.*
    FROM {{ref('usetracking_dashboard')}} u
    JOIN FacilityDates fd ON u.facility = fd.facility
    WHERE u.date_auto >= fd.min_date
)

SELECT 
    f.facility,
    SUM(f.total_use) as total_use,
    (fd.max_date - fd.min_date) AS days_range,
    ROUND((SUM(f.total_use) / (fd.max_date - fd.min_date + 1))::numeric / 1000, 1) || 'k' as average_use_per
FROM FilteredData f
JOIN FacilityDates fd ON f.facility = fd.facility
GROUP BY f.facility, fd.min_date, fd.max_date
ORDER BY f.facility



