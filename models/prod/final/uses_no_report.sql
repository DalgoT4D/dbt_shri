{{ config(
  materialized='table'
) }}

WITH Facilities AS (
    SELECT DISTINCT facility
    FROM {{ ref('usetracking_dashboard_new') }}
),
FacilityDates AS (
    SELECT 
        f.facility, 
        generate_series(
            (SELECT MIN(DATE(date_auto)) 
             FROM {{ ref('usetracking_dashboard_new') }} 
             WHERE facility = f.facility),
            CURRENT_DATE,
            INTERVAL '1 day'
        )::DATE AS date
    FROM Facilities f
),
SubmissionCounts AS (
    SELECT 
        facility, 
        DATE(date_auto) AS date_auto,
        COUNT(*) AS submission_count
    FROM {{ ref('usetracking_dashboard_new') }}
    GROUP BY facility, DATE(date_auto)
)

SELECT 
    fd.date AS date_auto,
    fd.facility
FROM FacilityDates fd
LEFT JOIN SubmissionCounts sc 
    ON fd.facility = sc.facility AND fd.date = sc.date_auto
WHERE fd.date <= CURRENT_DATE
  AND COALESCE(sc.submission_count, 0) = 0
ORDER BY fd.facility, fd.date