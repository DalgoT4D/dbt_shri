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

    -- Hardcoded inauguration dates per facility, mirroring daily_issue_form.sql
    facility_inauguration AS (
        SELECT facility, inauguration_date::date
        FROM (VALUES
            ('Dundibagh',                          '2022-02-19'),
            ('Basgoda',                            '2022-02-19'),
            ('Gomia',                              '2022-02-19'),
            ('Azad Nagar',                         '2022-08-23'),
            ('North Basgoda',                      '2022-08-23'),
            ('Peterbaar',                          '2023-01-14'),
            ('Vurahi',                             '2023-03-02'),
            ('Jaridih CSR',                        '2023-07-01'),
            ('Jaridih SBM',                        '2023-07-01'),
            ('Kasmar',                             '2023-07-01'),
            ('Nemua',                              '2023-10-29'),
            ('Bela Museri',                        '2023-12-04'),
            ('Bairo',                              '2023-11-01'),
            ('Karanpur',                           '2023-10-13'),
            ('Jiorid Koderma',                     '2026-07-31'),
            ('Arogya Mandir Koderma',              '2026-07-31'),
            ('PM SHRI Middle school Jaridih East', '2026-06-15')
        ) AS t(facility, inauguration_date)
    ),

    -- Fallback: earliest recorded date per facility for unlisted facilities
    facility_min_dates AS (
        SELECT facility, MIN(date_auto::date) AS first_seen_date
        FROM {{ ref('usetracking_dashboard_new') }}
        GROUP BY facility
    ),

    all_combinations AS (
        SELECT
            d.date,
            f.facility
        FROM date_series AS d
        CROSS JOIN facilities AS f
        LEFT JOIN facility_inauguration AS fi ON f.facility = fi.facility
        LEFT JOIN facility_min_dates AS fmd ON f.facility = fmd.facility
        WHERE d.date >= COALESCE(fi.inauguration_date, fmd.first_seen_date)
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
