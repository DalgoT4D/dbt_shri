{{ config(
  materialized='table'
) }}


-- SELECT facility, SUM(total_use) as total_use
--     FROM prod_final.usetracking_dashboard
--     WHERE 
--         EXTRACT(MONTH FROM date_auto) = 7
--          AND
--         EXTRACT(YEAR FROM date_auto) = 2023
--     GROUP BY facility

WITH mycte AS (
    SELECT
        facility,
        SUM(total_use) AS total_use
    FROM {{ ref('usetracking_dashboard') }}
    WHERE
        date_auto >= DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '3 months'
        AND date_auto < DATE_TRUNC('MONTH', CURRENT_DATE)
    GROUP BY facility
),

cost AS (
    SELECT * FROM {{ ref('facility_cost') }}
),

combine AS (
    SELECT 
        m.facility, 
        m.total_use,
        c.totals_inr,
        ROUND(
            CASE 
                WHEN m.total_use != 0 THEN CAST(c.totals_inr AS NUMERIC) / m.total_use
            END, 2
        ) AS inr_per_use,
        ROUND(
            CASE 
                WHEN m.total_use != 0 THEN (CAST(c.totals_inr AS NUMERIC) / m.total_use) / 83.24
            END, 2
        ) AS usd_per_use
    FROM mycte AS m
    LEFT JOIN cost AS c ON m.facility = c.facility
)

SELECT * FROM combine
