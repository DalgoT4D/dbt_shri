{{ config(
  materialized='table'
) }}


WITH date_range AS (
    SELECT generate_series(
        '2020-01-01'::date,  -- Start date
        '2024-12-31'::date,  -- End date
        '1 day'::interval    -- Interval of 1 day
    ) AS date_auto
),
facility_list AS (
    SELECT DISTINCT facility
    FROM {{ref('daily_issue_clean')}}
),
cross_product AS (
    SELECT dr.date_auto, fl.facility
    FROM date_range dr
    CROSS JOIN facility_list fl
),
issues_reported AS (
    SELECT 
        date_auto,
        facility,
        COUNT(issue) AS reported_issues_count
    FROM 
        {{ref('daily_issue_clean')}}
    WHERE
        issue IS NOT NULL AND issue <> '' -- This assumes an empty 'issue' column means no issue reported
    GROUP BY 
        date_auto,
        facility
)

SELECT 
    cast(cp.date_auto as date) as date_auto,
    cp.facility,
    COALESCE(ir.reported_issues_count, 0) AS no_of_reported_issues
FROM 
    cross_product cp
LEFT JOIN issues_reported ir ON cp.date_auto = ir.date_auto AND cp.facility = ir.facility
ORDER BY
    cp.facility, 
    cp.date_auto
