{{ config(
  materialized='table'
) }}

WITH 
SORTEDISSUES AS (
    -- Sort entries by facility, date, reverse shutdown status, and full_partial
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                FACILITY, DATE_AUTO
            ORDER BY
                FACILITY ASC, DATE_AUTO ASC, 
                CASE WHEN SHUTDOWN = 'yes' THEN 1 ELSE 0 END DESC, 
                CASE WHEN FULL_PARTIAL = 'full day' THEN 1 ELSE 0 END DESC, 
                TIME_AUTO
        ) AS NUM_PERDAY
    FROM {{ ref('daily_issue_clean') }}
),

FILTEREDISSUES AS (
    -- Keep only the first record for each facility-date combination
    SELECT * 
    FROM SORTEDISSUES
    WHERE NUM_PERDAY = 1
),

STATUSASSIGNED AS (
    -- Assign status according to the given rules
    SELECT 
        _ID, 
        FACILITY,
        DATE_AUTO,
        CASE
            WHEN DATE_AUTO IS NULL THEN 'no data'
            WHEN COALESCE(TRIM(SHUTDOWN), '') = '' THEN 'online' -- Handles NULL, empty, or spaces
            WHEN TRIM(SHUTDOWN) = 'no' THEN 'online'
            WHEN TRIM(SHUTDOWN) = 'yes' AND FULL_PARTIAL = 'part day' THEN 'part-day outage'
            WHEN TRIM(SHUTDOWN) = 'yes' AND FULL_PARTIAL = 'full day' THEN 'full-day outage'
            ELSE 'unknown' -- Catch-all condition, adjust as necessary
        END AS STATUS
    FROM FILTEREDISSUES 
),

RECENTISSUES AS (
    -- Only keep records within the last 70 days
    SELECT * 
    FROM STATUSASSIGNED
    WHERE DATE_AUTO > CURRENT_DATE - INTERVAL '70 days'
),

NUMBEREDRECENTISSUES AS (
    -- Number days and weeks
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY FACILITY
            ORDER BY DATE_AUTO
        ) AS DAY_SEQ,
        CEIL(ROW_NUMBER() OVER (
            PARTITION BY FACILITY
            ORDER BY DATE_AUTO
        ) / 7.0) AS WEEK_SEQ
    FROM RECENTISSUES
)

-- Final selection
SELECT 
    _ID,
    FACILITY, 
    DATE_AUTO, 
    STATUS,
    ((DAY_SEQ - 1) % 7) + 1 AS DAY_OF_THE_WEEK,
    WEEK_SEQ AS WEEK,
    1 AS BUBBLE 
FROM NUMBEREDRECENTISSUES
WHERE WEEK_SEQ <= 10
ORDER BY FACILITY, DATE_AUTO
