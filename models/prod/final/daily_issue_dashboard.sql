{{ config(
  materialized='table'
) }}

with my_cte as (
    {{ dbt_utils.union_relations(
        relations=[ref('daily_issue_dashboard_prework'), ref('daily_issue_form_others_issue')]
    ) }}
),

shutdown_cte as (
    select 
        COALESCE(_id::text, '') as _id,
        COALESCE(facility, '') as facility,
        date_auto,
        time_auto,
        COALESCE(category, '') as category,
        COALESCE(shift_type, '') as shift_type,
        COALESCE(issue, '') as issue,
        COALESCE(fixed, '') as fixed,
        COALESCE(full_partial, '') as full_partial,
        COALESCE(num_hours::text, '') as num_hours,
        COALESCE(shutdown, '') as shutdown,
        CASE 
            WHEN shutdown = 'yes' AND num_hours::float >= 8 THEN 'full shift'
            WHEN shutdown = 'yes' AND num_hours::float < 8 THEN 'partial shift'
            ELSE 'no'
        END AS shift_shutdown
    from my_cte
    where category <> 'other'
),

day_agg as (
    select
        facility,
        date_auto,
        array_agg(distinct shift_shutdown) as shutdown_statuses
    from shutdown_cte
    group by facility, date_auto
),

day_shutdown_final as (
    select
        facility,
        date_auto,
        case
            when array['full shift'] <@ shutdown_statuses and cardinality(shutdown_statuses) = 1 then 'full day'
            when array['no'] <@ shutdown_statuses and cardinality(shutdown_statuses) = 1 then 'no'
            else 'partial day'
        end as day_shutdown
    from day_agg
),

final as (
    select 
        s.*,
        d.day_shutdown
    from shutdown_cte s
    left join day_shutdown_final d
    on s.facility = d.facility and s.date_auto = d.date_auto
)

select * from final