
{{ config(
  materialized='table',
   indexes=[
      {'columns': ['_airbyte_ab_id'], 'type': 'hash'}
    ],
    schema='intermediate'

) }}

with 
   daily_issue as (select
   {{ dbt_utils.star(from= ref('daily_issue_form_normalized'), except=['_notes', '_geolocation', '_version_']) }},
    '' as facility
   from {{ ref('daily_issue_form_normalized') }} ),

   form_kd as 
   (select
   {{ dbt_utils.star(from= ref('enrollment_normalized'), except=['_notes', '_geolocation', '_version_']) }}
   from {{ ref('enrollment_normalized') }} ),

   join_all_tables as (
    select *, 
    form_kd.facilityname,
    '' as facilityname
    from daily_issue
    left join form_kd
    ON daily_issue.facilityname = form_kd.facilityname
   )

select * from join_all_tables