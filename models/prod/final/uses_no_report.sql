{{ config(
  materialized='table'
) }}

-- Determine the dynamic range of dates based on the existing data
WITH cte AS (
    WITH dynamic_range AS (
        SELECT
            MIN(date_auto::date) AS start_date,
            MAX(date_auto::date) AS end_date
        FROM {{ ref('usetracking_dashboard_new') }}
    ),

    date_series AS (
        SELECT GENERATE_SERIES(start_date, end_date, '1 day'::interval) AS date
        FROM dynamic_range
    ),

    facilities AS (
        SELECT DISTINCT facility FROM {{ ref('usetracking_dashboard_new') }}
    ),

    all_combinations AS (
        SELECT
            d.date,
            f.facility
        FROM date_series AS d
        CROSS JOIN facilities AS f
    ),

    data_counts AS (
        SELECT
            date_auto::date AS date_auto,
            facility,
            COUNT(*) AS num_entries
        FROM {{ ref('usetracking_dashboard_new') }}
        GROUP BY date_auto::date, facility
    )

    SELECT
        ac.date,
        ac.facility
    FROM all_combinations AS ac
    LEFT JOIN data_counts AS dc 
        ON ac.date = dc.date_auto AND ac.facility = dc.facility
    WHERE dc.num_entries IS NULL OR dc.num_entries = 0
)

SELECT
    date::date AS date_auto, 
    facility 
FROM cte
WHERE date <= CURRENT_DATE 
