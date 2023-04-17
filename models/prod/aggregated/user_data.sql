
{{ config(
  materialized='table',
  schema='aggregated'
) }}


with my_cte as (select userid, 
max(date_auto) as last_use_date,
min(date_auto) as first_use_date, 
COUNT(userid) as n_by_id
from  {{ref('use_tracking')}} 
GROUP BY userid
ORDER BY userid),

user_data as (select a.*,
   {{ dbt_utils.star(from= ref('enrollment_production'), except=['userid']) }},
   date_part('day', CURRENT_DATE - b.date_enrollment::timestamp) as days_since_enroll
from my_cte as a
left join {{ref('enrollment_production')}} as b
on a.userid = b.userid)

select *,
   {{ dbt_utils.safe_divide('n_by_id', 'days_since_enroll') }} as avg_useperday
from user_data 


