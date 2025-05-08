{{ config(
  materialized='table'
) }}


WITH 
mycte AS (
    SELECT 
        facility,
        date_auto
    FROM {{ ref('usetracking_dashboard') }}
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

    UNION ALL

    -- Logic for facilities NOT in the specified list
    SELECT 
        facility,
        date_auto
    FROM {{ ref('usetracking_dashboard') }}
    WHERE 
        facility NOT IN ('Dundibagh', 'Basgoda', 'Gomia', 'Azad Nagar', 'North Basgoda', 'Peterbaar', 'Jaridih CSR', 'Jaridih SBM', 'Kasmar', 'Vurahi')
    GROUP BY facility, date_auto
),

facilitydates AS (
    SELECT 
        facility,
        MIN(date_auto) AS min_date,
        MAX(date_auto) AS max_date
    FROM mycte
    GROUP BY facility
),

filtereddata AS (
    SELECT  u.*
    FROM {{ ref('usetracking_dashboard') }} AS u
    INNER JOIN facilitydates AS fd ON u.facility = fd.facility
    WHERE u.date_auto >= fd.min_date
)

SELECT 
    f.facility,
    SUM(f.total_use) AS total_use,
    (fd.max_date - fd.min_date) AS days_range,
    ROUND((SUM(f.total_use) / (fd.max_date - fd.min_date + 1))::numeric, 0) AS average_use_per -- Removed /1000 to get the full value
FROM filtereddata AS f
INNER JOIN facilitydates AS fd ON f.facility = fd.facility
GROUP BY f.facility, fd.min_date, fd.max_date
ORDER BY f.facility
