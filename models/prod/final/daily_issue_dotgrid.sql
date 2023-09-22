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
),
NumberedRecentIssues AS (
    -- Number days and weeks
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY facility ORDER BY date_auto) AS day_seq,
           CEIL(ROW_NUMBER() OVER (PARTITION BY facility ORDER BY date_auto) / 7.0) AS week_seq
    FROM RecentIssues
)
-- Final selection
SELECT 
    facility, 
    date_auto, 
    status,
    ((day_seq - 1) % 7) + 1 AS day_of_the_week,
    week_seq AS week,
    1 AS bubble 
FROM NumberedRecentIssues
ORDER BY facility, date_auto
