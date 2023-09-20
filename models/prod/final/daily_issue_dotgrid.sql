{{ config(
  materialized='table'
) }}


WITH NumberedIssues AS (
    -- Number each entry per facility and date, ordering by shutdown
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY facility, date_auto ORDER BY time_auto) AS num_perday
    FROM {{ref('daily_issue_clean')}}
),
FilteredIssues AS (
    -- Only keep the first record for each facility-date
    SELECT * 
    FROM NumberedIssues
    WHERE num_perday = 1
),
StatusAssigned AS (
    -- Assign status according to the given rules
    SELECT 
        facility,
        date_auto,
        CASE
            WHEN date_auto IS NULL THEN 'no data'
            WHEN shutdown IS NULL OR shutdown = 'no' THEN 'online'
            WHEN shutdown = 'yes' AND full_partial = 'part day' THEN 'part-day outage'
            WHEN shutdown = 'yes' AND full_partial = 'full day' THEN 'full-day outage'
            ELSE 'unknown' -- Catch-all condition, adjust as necessary
        END AS status
    FROM FilteredIssues
),
RecentIssues AS (
    -- Only keep records within the last 71 days
    SELECT * 
    FROM StatusAssigned
    WHERE date_auto > CURRENT_DATE - INTERVAL '71 days'
)
-- Final selection
SELECT facility, date_auto, status,
EXTRACT(DOW FROM date_auto) AS day_of_the_week,
DATE_PART('week', date_auto) AS week
FROM RecentIssues
