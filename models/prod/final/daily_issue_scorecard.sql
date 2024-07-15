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
    FROM {{ref('daily_issue_clean')}}
    WHERE issue != ''
),
FullDayOutages AS (
    -- Count full-day shutdown instances for each facility within the last 10 weeks
    SELECT 
        facility, 
        COUNT(*) AS num_last10_full
    FROM CleanedData, DateCalculations
    WHERE shutdown = 'yes' 
    AND full_partial = 'full day'
    AND date_auto >= wks_10
    GROUP BY facility
),
PartialDayOutages AS (
    -- Sum partial-day shutdown hours for each facility within the last 10 weeks
    SELECT 
        facility, 
        COALESCE(SUM(CAST(num_hours AS NUMERIC)), 0) / 16 AS num_last10_partialhrs_days
    FROM CleanedData, DateCalculations
    WHERE shutdown = 'yes' 
    AND full_partial = 'part day'
    AND date_auto >= wks_10
    GROUP BY facility
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
    FROM {{ref('daily_issue_clean')}}
    WHERE shutdown = 'yes'
    GROUP BY facility
),
FirstQuality AS (
    -- Get the first date for each facility
    SELECT 
        facility, 
        MIN(date_auto) AS first_quality
    FROM {{ref('daily_issue_clean')}}
    GROUP BY facility
),
FinalData AS (
    -- Combine the above data to compute the necessary columns
    SELECT 
        fq.facility, 
        ROUND(100 - 100 * COALESCE(s.num_last10_totaldays, 0) / 70, 2) AS last_10_weeks,
        COALESCE(l.last_outage, fq.first_quality) AS continuous_since,
        CASE 
            WHEN l.last_outage IS NOT NULL THEN (d.today - l.last_outage)
            ELSE (d.today - fq.first_quality)
        END AS days_continuous_operation
    FROM FirstQuality fq
    JOIN DateCalculations d ON TRUE
    LEFT JOIN ShutdownCounts s ON fq.facility = s.facility
    LEFT JOIN LastOutage l ON fq.facility = l.facility
)
-- Select the final output
SELECT 
    facility,
    last_10_weeks,
    continuous_since,
    days_continuous_operation
FROM FinalData
ORDER BY facility
