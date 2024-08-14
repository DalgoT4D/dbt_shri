{{ config(
  materialized='table'
) }}

WITH 
SortedIssues AS (
    -- Sort entries by facility, date, reverse shutdown status, and full_partial
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY facility, date_auto ORDER BY facility, date_auto, 
                              CASE WHEN shutdown = 'yes' THEN 1 ELSE 0 END DESC, 
                              CASE WHEN full_partial = 'full day' THEN 1 ELSE 0 END DESC, 
                              time_auto) AS num_perday
    FROM {{ref('daily_issue_clean')}}
),
FilteredIssues AS (
    -- Keep only the first record for each facility-date combination
    SELECT * 
    FROM SortedIssues
    WHERE num_perday = 1
),
StatusAssigned AS (
    -- Assign status according to the given rules
    SELECT 
        _id, 
        facility,
        date_auto,
        CASE
            WHEN date_auto IS NULL THEN 'no data'
            WHEN COALESCE(TRIM(shutdown), '') = '' THEN 'online' -- Handles NULL, empty, or spaces
            WHEN TRIM(shutdown) = 'no' THEN 'online'
            WHEN TRIM(shutdown) = 'yes' AND full_partial = 'part day' THEN 'part-day outage'
            WHEN TRIM(shutdown) = 'yes' AND full_partial = 'full day' THEN 'full-day outage'
            ELSE 'unknown' -- Catch-all condition, adjust as necessary
        END AS status
    FROM FilteredIssues 
),
RecentIssues AS (
    -- Only keep records within the last 70 days
    SELECT * 
    FROM StatusAssigned
    WHERE date_auto > CURRENT_DATE - INTERVAL '70 days'
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
    _id,
    facility, 
    date_auto, 
    status,
    ((day_seq - 1) % 7) + 1 AS day_of_the_week,
    week_seq AS week,
    1 AS bubble 
FROM NumberedRecentIssues
WHERE week_seq <= 10
ORDER BY facility, date_auto
