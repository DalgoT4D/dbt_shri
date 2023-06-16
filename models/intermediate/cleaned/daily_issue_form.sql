{{ config(
  materialized='table',
    schema='intermediate'

) }}


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

with 
   daily_issue as (select
   coalesce(other_group_outage_other, '0') as other_group_outage_other,
   {{ dbt_utils.star(from= ref('daily_issue_form_normalized'), 
   except=['_airbyte_ab_id', 
           '_notes', 
           '_geolocation', 
           '_version_',
           '_xform_id_string',
           '_tags',
           '_status',
           'attachments',
           'meta_deprecatedid',
           '_validation_status',
           'meta_instancename',
           'other_group_outage_other']) }}
   from {{ ref('daily_issue_form_normalized') }} ),

   form_kd as 
   (select
   {{ dbt_utils.star(from= ref('facility_koboid_link_normalized'), 
   except=['_notes', 
           '_geolocation', 
           '_version_', 
           '_xform_id_string', 
           '_tags', 
           '_status']) }}
   from {{ ref('facility_koboid_link_normalized') }} ),

   join_all_tables as (
    select daily_issue.*, 
    form_kd.facilityname
    from daily_issue
    left join form_kd
    ON daily_issue._submitted_by = form_kd.kobo_username
   )

select * from join_all_tables