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
ShutdownCounts AS (
    -- Count shutdown instances for each facility within the last 10 weeks
    SELECT 
        facility, 
        COUNT(*) FILTER (WHERE shutdown = 'yes' AND date_auto > wks_10) AS num
    FROM prod_final.daily_issue_clean, DateCalculations
    WHERE shutdown != '' AND shutdown != 'no'
    GROUP BY facility
),
LastOutage AS (
    -- Get the date of the last outage for each facility
    SELECT
        facility,
        MAX(date_auto) AS last_outage
    FROM prod_final.daily_issue_clean
    WHERE shutdown = 'yes'
    GROUP BY facility
),
FirstQuality AS (
    -- Get the first date for each facility
    SELECT 
        facility, 
        MIN(date_auto) AS first_quality
    FROM prod_final.daily_issue_clean
    GROUP BY facility
),
FinalData AS (
    -- Combine the above data to compute the necessary columns
    SELECT 
        fq.facility, 
        COALESCE(ROUND(100 - s.num::decimal / 70, 2), 100) AS last_10_weeks,
        COALESCE(l.last_outage, fq.first_quality) AS continuous_since,
        CASE 
            WHEN l.last_outage IS NOT NULL THEN -(l.last_outage - d.today)
            ELSE -(fq.first_quality - d.today)
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
