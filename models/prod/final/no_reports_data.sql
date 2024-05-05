{{ config(
  materialized='table'
) }}


WITH DateRange AS (
    SELECT generate_series(MIN(date_auto), MAX(date_auto), interval '1 day') AS date
    FROM prod_final.daily_issue_clean
), Facilities AS (
    SELECT DISTINCT facility
    FROM prod_final.daily_issue_clean
), FacilityDates AS (
    SELECT f.facility, d.date
    FROM Facilities f
    CROSS JOIN DateRange d
)
SELECT fd.facility, CAST(fd.date AS date) AS date, COUNT(d._id) AS submission_count
FROM FacilityDates fd
LEFT JOIN prod_final.daily_issue_clean d ON fd.facility = d.facility AND fd.date = d.date_auto
WHERE fd.date <= CURRENT_DATE  
GROUP BY fd.facility, fd.date
HAVING COUNT(d._id) < 2
