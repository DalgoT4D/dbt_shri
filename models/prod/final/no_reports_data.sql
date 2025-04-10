{{ config(
  materialized='table'
) }}

WITH Facilities AS (
    SELECT DISTINCT facility
    FROM {{ ref('daily_issue_clean') }}
), 
FacilityDates AS (
    SELECT 
        f.facility, 
        generate_series(
            (SELECT MIN(DATE(date_auto)) 
             FROM {{ ref('daily_issue_clean') }} 
             WHERE facility = f.facility),
            CURRENT_DATE,
            INTERVAL '1 day'
        )::DATE AS date
    FROM Facilities f
),
SubmissionCounts AS (
    SELECT 
        facility, 
        DATE(date_auto) AS date_auto,  -- Ensure date is without time
        COUNT(*) AS submission_count
    FROM {{ ref('daily_issue_clean') }}
    GROUP BY facility, DATE(date_auto)
)

SELECT 
    fd.facility, 
    fd.date::DATE AS date,  -- Ensure final output is a pure DATE type
    COALESCE(sc.submission_count, 0) AS submission_count
FROM FacilityDates fd
LEFT JOIN SubmissionCounts sc 
ON fd.facility = sc.facility 
AND fd.date = sc.date_auto
WHERE fd.date <= CURRENT_DATE::DATE  -- Ensure comparison is done on DATE type
AND COALESCE(sc.submission_count, 0) <> 2
ORDER BY fd.facility, fd.date
