{{ config(
  materialized='table'
) }}

WITH 
DateCalculations AS (
    -- Calculate today and 10 weeks ago
    SELECT 
        CURRENT_DATE AS today,
        CURRENT_DATE - INTERVAL '70 days' AS wks_10
),
CleanedData AS (
    -- Drop records with no issue
    SELECT *
    FROM {{ ref('daily_issue_clean') }}
    WHERE issue != ''
),
FullDayOutages AS (
    -- Step 1: Count full-day shutdown instances for each facility within the last 10 weeks
    SELECT 
        facility, 
        COUNT(DISTINCT date_auto) AS num_last10_full
    FROM CleanedData, DateCalculations
    WHERE shutdown = 'yes' 
    AND full_partial = 'full day'
    AND date_auto >= wks_10
    GROUP BY facility
),
PartialDayShutdownHours AS (
    -- Step 2: Sum partial-day shutdown hours for each facility within the last 10 weeks
    -- Only include days without full shutdowns
    SELECT 
        cd.facility, 
        COALESCE(SUM(CAST(cd.num_hours AS NUMERIC)), 0) AS total_partial_shutdown_hours
    FROM CleanedData cd
    LEFT JOIN (
        SELECT facility, date_auto
        FROM CleanedData
        WHERE shutdown = 'yes' AND full_partial = 'full day' AND date_auto >= (SELECT wks_10 FROM DateCalculations)
        GROUP BY facility, date_auto
    ) fds ON cd.facility = fds.facility AND cd.date_auto = fds.date_auto
    WHERE cd.shutdown = 'yes'
    AND cd.full_partial = 'part day'
    AND cd.date_auto >= (SELECT wks_10 FROM DateCalculations)
    AND fds.date_auto IS NULL
    GROUP BY cd.facility
),
PartialDayOutages AS (
    -- Step 3: Divide the total number of hours of partial shutdowns by 16
    SELECT 
        facility, 
        total_partial_shutdown_hours / 16 AS num_last10_partialhrs_days
    FROM PartialDayShutdownHours
),
ShutdownCounts AS (
    -- Combine full-day and partial-day outages
    SELECT 
        COALESCE(f.facility, p.facility) AS facility,
        COALESCE(f.num_last10_full, 0) AS num_last10_full, 
        COALESCE(p.num_last10_partialhrs_days, 0) AS num_last10_partialhrs_days,
        (COALESCE(f.num_last10_full, 0) + COALESCE(p.num_last10_partialhrs_days, 0)) AS num_last10_totaldays
    FROM FullDayOutages f
    FULL OUTER JOIN PartialDayOutages p ON f.facility = p.facility
),
LastOutage AS (
    -- Get the date of the last outage for each facility
    SELECT
        facility,
        MAX(date_auto) AS last_outage
    FROM {{ ref('daily_issue_clean') }}
    WHERE shutdown = 'yes'
    GROUP BY facility
),
FirstQuality AS (
    -- Get the first date for each facility
    SELECT 
        facility, 
        MIN(date_auto) AS first_quality
    FROM {{ ref('daily_issue_clean') }}
    GROUP BY facility
),
FinalData AS (
    -- Combine the above data to compute the necessary columns
    SELECT 
        fq.facility, 
        ROUND(100 - 100 * COALESCE(s.num_last10_totaldays, 0) / 70, 2) AS last_10_weeks,
        COALESCE(lo.last_outage, fq.first_quality) AS continuous_since,
        ROUND((d.today - COALESCE(lo.last_outage, fq.first_quality))) AS days_continuous_operation
    FROM FirstQuality fq
    JOIN DateCalculations d ON TRUE
    LEFT JOIN ShutdownCounts s ON fq.facility = s.facility
    LEFT JOIN LastOutage lo ON fq.facility = lo.facility
)
-- Select the final output
SELECT 
    facility,
    last_10_weeks,
    continuous_since,
    days_continuous_operation
FROM FinalData
ORDER BY facility
