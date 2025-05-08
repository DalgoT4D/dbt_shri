-- 1. Configuration: The first line sets the configuration options for the subsequent code. 
--    It specifies that the resulting table should be materialized as a regular table in the 'intermediate' schema.

-- 2. Common Table Expressions (CTEs): CTEs are temporary result sets that can be referenced within the query. 
--    In this code, there are three CTEs defined: `daily_issue`, `form_kd`, and `join_all_tables`.

--    - `daily_issue`: It selects columns from a table called 'daily_issue_form_normalized' and renames one of the columns using the `coalesce` function. 
--       It retrieves all columns except for a list of excluded columns.
   
--    - `form_kd`: It selects columns from a table called 'facility_koboid_link_normalized' and retrieves all columns except for a list of excluded columns.
   
--    - `join_all_tables`: It joins the previous two CTEs (`daily_issue` and `form_kd`) based on a common column `_submitted_by` and selects all columns from both CTEs.

-- 3. Final Query: The last line of code `select * from join_all_tables` executes the final query, which selects all columns from the `join_all_tables` CTE.

-- In summary, this code combines data from two tables, `daily_issue_form_normalized` and `facility_koboid_link_normalized`, using a left join based on a common column. 

-- Read about left join here ->>>>> https://www.tutorialspoint.com/sql/sql-left-joins.htm
{{ config(
  materialized='table'
) }}

WITH 
daily_issue AS (
    SELECT
        coalesce(other_group_outage_other, '0') AS other_group_outage_other,
        coalesce(to_date(date, 'YYYY-MM-DD'), to_date(timestamp_formatted, 'YYYY-MM-DD')) AS date_auto,
        coalesce(cast(left(time, 5) AS time), cast(date_trunc('minute', cast(timestamp_formatted AS timestamp)) AS time)) AS time_auto,
           {{ dbt_utils.star(from= ref('daily_issue_form_normalized'), 
             except=['_airbyte_raw_id', '_notes', '_geolocation', '_version_', 
                     '_xform_id_string', '_tags', '_status', 'attachments', 
                     'meta_deprecatedid', '_validation_status', 'meta_instancename', 
                     'other_group_outage_other', 'facilityname']) }} -- Exclude 'facilityname'
    FROM {{ ref('daily_issue_form_normalized') }}
),

form_kd AS (
    SELECT
           {{ dbt_utils.star(from= ref('facility_koboid_link_normalized'), 
             except=['_notes', '_geolocation', '_version_', '_xform_id_string', '_tags', '_status']) }}
    FROM {{ ref('facility_koboid_link_normalized') }}
),

join_all_tables AS (
    SELECT 
        di.*, 
        fk.facilityname
    FROM daily_issue AS di
    RIGHT JOIN form_kd AS fk 
        ON di._submitted_by = fk.kobo_username
),

min_dates AS (
    -- Subquery to get the minimum date_auto for each facility
    SELECT 
        facilityname, 
        min(date_auto) AS min_date_auto
    FROM join_all_tables
    WHERE date_auto IS NOT NULL
    GROUP BY facilityname
)

SELECT
    jt.*,
    md.min_date_auto
FROM join_all_tables AS jt
LEFT JOIN min_dates AS md 
    ON jt.facilityname = md.facilityname
WHERE
    jt.date_auto IS NOT NULL 
    AND jt.time_auto IS NOT NULL
    AND (
        -- Explicit conditions for known facilities
        (jt.facilityname = 'Dundibagh' AND jt.date_auto >= '2022-02-19')
        OR (jt.facilityname = 'Basgoda' AND jt.date_auto >= '2022-02-19')
        OR (jt.facilityname = 'Gomia' AND jt.date_auto >= '2022-02-19')
        OR (jt.facilityname = 'Azad Nagar' AND jt.date_auto >= '2022-08-23')
        OR (jt.facilityname = 'North Basgoda' AND jt.date_auto >= '2022-08-23')
        OR (jt.facilityname = 'Peterbaar' AND jt.date_auto >= '2023-01-14')
        OR (jt.facilityname = 'Vurahi' AND jt.date_auto >= '2023-03-02')
        OR (jt.facilityname = 'Jaridih CSR' AND jt.date_auto >= '2023-07-01')
        OR (jt.facilityname = 'Jaridih SBM' AND jt.date_auto >= '2023-07-01')
        OR (jt.facilityname = 'Kasmar' AND jt.date_auto >= '2023-07-01')
        OR (jt.facilityname = 'Nemua' AND jt.date_auto >= '2023-10-29')
        OR (jt.facilityname = 'Bela Museri' AND jt.date_auto >= '2023-12-04')
        OR (jt.facilityname = 'Bairo' AND jt.date_auto >= '2023-11-01')
        OR (jt.facilityname = 'Karanpur' AND jt.date_auto >= '2023-10-13')
        -- Default condition for new facilities: Use their minimum date_auto
        OR (jt.date_auto >= md.min_date_auto)
    )
