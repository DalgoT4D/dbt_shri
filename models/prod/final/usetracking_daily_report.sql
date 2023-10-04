{{ config(
  materialized='table'
) }}

select 
total_use, 
men_regular_number + women_regular_number as regular_users,
girl_number + boy_number + man_number + woman_number as non_regular_users, 
men_use as men, 
women_use as women,
boys_use as boys,
girls_use as girls
FROM {{ref('usetracking_dashboard')}}