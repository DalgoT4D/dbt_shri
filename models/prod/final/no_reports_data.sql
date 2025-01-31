{{ config(
  materialized='table'
) }}


WITH DateRange AS (
    SELECT generate_series(MIN(DATE(date_auto)), MAX(DATE(date_auto)), interval '1 day') AS date
    FROM {{ref('daily_issue_clean')}}
), 
Facilities AS (
    SELECT DISTINCT facility
    FROM {{ref('daily_issue_clean')}}
), 
FacilityDates AS (
    SELECT f.facility, d.date
    FROM Facilities f
    CROSS JOIN DateRange d
), 
SubmissionCounts AS (
    SELECT facility, DATE(date_auto) AS date_auto, COUNT(*) AS submission_count
    FROM {{ref('daily_issue_clean')}}
    GROUP BY facility, DATE(date_auto)
)

SELECT fd.facility, fd.date, COALESCE(sc.submission_count, 0) AS submission_count
FROM FacilityDates fd
LEFT JOIN SubmissionCounts sc 
ON fd.facility = sc.facility 
AND fd.date = sc.date_auto
WHERE fd.date <= CURRENT_DATE
AND COALESCE(sc.submission_count, 0) < 2
ORDER BY fd.facility, fd.date
