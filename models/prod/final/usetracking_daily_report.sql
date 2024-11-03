{{ config(
  materialized='table'
) }}

SELECT 
  total_use, 
  men_regular_number + women_regular_number AS regular_users,
  girl_number + boy_number + man_number + woman_number AS non_regular_users, 
  man_number AS men, 
  woman_number AS women,
  boys_use AS boys,
  girls_use AS girls,
  highlow_usage_level   -- Include low use count in the report
FROM {{ ref('usetracking_dashboard') }}
