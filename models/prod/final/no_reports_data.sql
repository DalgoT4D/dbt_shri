{{ config(
  materialized='table'
) }}

WITH facilities AS (
    SELECT DISTINCT facility
    FROM {{ ref('daily_issue_clean') }}
), 

facility_dates AS (
    SELECT 
        facility, 
        generate_series(
            (
                SELECT min(date(date_auto)) 
                FROM {{ ref('daily_issue_clean') }} 
                WHERE facility = f.facility
            ),
            current_date,
            INTERVAL '1 day'
        )::DATE AS date
    FROM facilities AS f
),

submission_counts AS (
    SELECT 
        facility, 
        date(date_auto) AS date_auto,
        count(DISTINCT time_auto) AS submission_count
    FROM {{ ref('daily_issue_clean') }}
    GROUP BY facility, date(date_auto)
)

SELECT 
    fd.facility, 
    fd.date,
    coalesce(sc.submission_count, 0) AS submission_count
FROM facility_dates AS fd
LEFT JOIN submission_counts AS sc 
    ON
        fd.facility = sc.facility 
        AND fd.date = sc.date_auto
WHERE
    fd.date <= current_date
    AND coalesce(sc.submission_count, 0) <> 2
ORDER BY fd.facility, fd.date
