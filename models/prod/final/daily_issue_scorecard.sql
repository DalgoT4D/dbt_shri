{{ config(
  materialized='table'
) }}

WITH 
DATECALCULATIONS AS (
    -- Calculate today and 10 weeks ago
    SELECT 
        CURRENT_DATE AS TODAY,
        CURRENT_DATE - INTERVAL '70 days' AS WKS_10
),

CLEANEDDATA AS (
    -- Drop records with no issue
    SELECT *
    FROM {{ ref('daily_issue_clean') }}
    WHERE ISSUE != ''
),

FULLDAYOUTAGES AS (
    -- Step 1: Count full-day shutdown instances for each facility within the last 10 weeks
    SELECT 
        FACILITY, 
        COUNT(DISTINCT DATE_AUTO) AS NUM_LAST10_FULL
    FROM CLEANEDDATA, DATECALCULATIONS
    WHERE
        SHUTDOWN = 'yes' 
        AND FULL_PARTIAL = 'full day'
        AND DATE_AUTO >= WKS_10
    GROUP BY FACILITY
),

PARTIALDAYSHUTDOWNHOURS AS (
    -- Step 2: Sum partial-day shutdown hours for each facility within the last 10 weeks
    -- Only include days without full shutdowns
    SELECT 
        CD.FACILITY, 
        COALESCE(SUM(CAST(CD.NUM_HOURS AS NUMERIC)), 0) AS TOTAL_PARTIAL_SHUTDOWN_HOURS
    FROM CLEANEDDATA AS CD
    LEFT JOIN (
        SELECT
            FACILITY,
            DATE_AUTO
        FROM CLEANEDDATA
        WHERE SHUTDOWN = 'yes' AND FULL_PARTIAL = 'full day' AND DATE_AUTO >= (SELECT WKS_10 FROM DATECALCULATIONS)
        GROUP BY FACILITY, DATE_AUTO
    ) AS FDS ON CD.FACILITY = FDS.FACILITY AND CD.DATE_AUTO = FDS.DATE_AUTO
    WHERE
        CD.SHUTDOWN = 'yes'
        AND CD.FULL_PARTIAL = 'part day'
        AND CD.DATE_AUTO >= (SELECT WKS_10 FROM DATECALCULATIONS)
        AND FDS.DATE_AUTO IS NULL
    GROUP BY CD.FACILITY
),

PARTIALDAYOUTAGES AS (
    -- Step 3: Divide the total number of hours of partial shutdowns by 16
    SELECT 
        FACILITY, 
        TOTAL_PARTIAL_SHUTDOWN_HOURS / 16 AS NUM_LAST10_PARTIALHRS_DAYS
    FROM PARTIALDAYSHUTDOWNHOURS
),

SHUTDOWNCOUNTS AS (
    -- Combine full-day and partial-day outages
    SELECT 
        COALESCE(F.FACILITY, P.FACILITY) AS FACILITY,
        COALESCE(F.NUM_LAST10_FULL, 0) AS NUM_LAST10_FULL, 
        COALESCE(P.NUM_LAST10_PARTIALHRS_DAYS, 0) AS NUM_LAST10_PARTIALHRS_DAYS,
        (COALESCE(F.NUM_LAST10_FULL, 0) + COALESCE(P.NUM_LAST10_PARTIALHRS_DAYS, 0)) AS NUM_LAST10_TOTALDAYS
    FROM FULLDAYOUTAGES AS F
    FULL OUTER JOIN PARTIALDAYOUTAGES AS P ON F.FACILITY = P.FACILITY
),

LASTOUTAGE AS (
    -- Get the date of the last outage for each facility
    SELECT
        FACILITY,
        MAX(DATE_AUTO) AS LAST_OUTAGE
    FROM {{ ref('daily_issue_clean') }}
    WHERE SHUTDOWN = 'yes'
    GROUP BY FACILITY
),

FIRSTQUALITY AS (
    -- Get the first date for each facility
    SELECT 
        FACILITY, 
        MIN(DATE_AUTO) AS FIRST_QUALITY
    FROM {{ ref('daily_issue_clean') }}
    GROUP BY FACILITY
),

FINALDATA AS (
    -- Combine the above data to compute the necessary columns
    SELECT 
        FQ.FACILITY, 
        ROUND(100 - 100 * COALESCE(S.NUM_LAST10_TOTALDAYS, 0) / 70, 2) AS LAST_10_WEEKS,
        COALESCE(LO.LAST_OUTAGE, FQ.FIRST_QUALITY) AS CONTINUOUS_SINCE,
        ROUND((D.TODAY - COALESCE(LO.LAST_OUTAGE, FQ.FIRST_QUALITY))) AS DAYS_CONTINUOUS_OPERATION
    FROM FIRSTQUALITY AS FQ
    INNER JOIN DATECALCULATIONS AS D ON TRUE
    LEFT JOIN SHUTDOWNCOUNTS AS S ON FQ.FACILITY = S.FACILITY
    LEFT JOIN LASTOUTAGE AS LO ON FQ.FACILITY = LO.FACILITY
)

-- Select the final output
SELECT 
    FACILITY,
    LAST_10_WEEKS,
    CONTINUOUS_SINCE,
    DAYS_CONTINUOUS_OPERATION
FROM FINALDATA
ORDER BY FACILITY
