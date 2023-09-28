{{ config(
  materialized='table'
) }}
with mycte as (
    SELECT facility, SUM(total_use) as total_use
    FROM prod_final.usetracking_dashboard
    WHERE date_auto BETWEEN (CURRENT_DATE - INTERVAL '3 months') AND CURRENT_DATE
    GROUP BY facility
),
cost as (
    select * from {{ref('facility_cost')}}
),
combine as (
    SELECT 
        m.facility, 
        m.total_use,
        c.totals_inr,
        CASE 
            WHEN m.total_use != 0 THEN CAST(c.totals_inr AS numeric) / m.total_use
            ELSE NULL
        END AS inr_per_use
    FROM mycte m
    LEFT JOIN cost c ON m.facility = c.facility
)

SELECT * FROM combine
