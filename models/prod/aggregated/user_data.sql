{{ config(
  materialized='table'
) }}


with my_cte as (
    select
        userid, 
        max(date_auto) as last_use_date,
        min(date_auto) as first_use_date, 
        count(userid) as n_by_id
    from  {{ ref('use_tracking') }} 
    group by userid
    order by userid
),

user_data as (
    select
        a.*,
        {{ dbt_utils.star(from= ref('enrollment_gender_nodup'), except=['userid']) }},
        date_part('day', current_date - b.date_enrollment::timestamp) as days_since_enroll
    from my_cte as a
    left join {{ ref('enrollment_gender_nodup') }} as b
        on a.userid = b.userid
)

select
    *,
    {{ dbt_utils.safe_divide('n_by_id', 'days_since_enroll') }} as avg_useperday
from user_data 
