
{{ config(
  materialized='table',
    schema='intermediate'

) }}

with 
   daily_issue as (select
   {{ dbt_utils.star(from= ref('daily_issue_form_normalized'), except=['_airbyte_ab_id', '_notes', '_geolocation', '_version_']) }}
   from {{ ref('daily_issue_form_normalized') }} ),

   form_kd as 
   (select
   {{ dbt_utils.star(from= ref('facility_koboid_link_normalized'), except=['_notes', '_geolocation', '_version_', '_xform_id_string', '_tags', '_status']) }}
   from {{ ref('facility_koboid_link_normalized') }} ),

   join_all_tables as (
    select daily_issue.*, 
    form_kd.facilityname
    from daily_issue
    right join form_kd
    ON daily_issue._submitted_by = form_kd.kobo_username
   )

select * from join_all_tables