
{{ config(
  materialized='table'
) }}



with my_cte as (select date_auto, facility, 
sum(
    case when gender = 'male' then 1 
    else 0 end
) as men_regular_number, 
sum(
    case when gender = 'female' then 1 
    else 0 end
) as women_regular_number

 from {{ref('use_tracking')}}
 GROUP BY
    date_auto, facility
 order by date_auto desc)

SELECT 
  COALESCE(a.facility, b.facility) AS facility, 
  COALESCE(a.date_auto, b.date_auto) AS date_auto,
  a.men_regular_number,
  a.women_regular_number,
  COALESCE(b.girl_number, 0) AS girl_number, 
  COALESCE(b.boy_number, 0) AS boy_number,
  COALESCE(b.woman_number, 0) AS woman_number,
  COALESCE(b.man_number, 0) AS man_number,
  COALESCE(a.women_regular_number, 0) + COALESCE(b.woman_number, 0) AS women_use,
  COALESCE(b.girl_number, 0) AS girls_use,
  COALESCE(a.men_regular_number, 0) + COALESCE(b.man_number, 0) AS men_use,
  COALESCE(b.boy_number, 0) AS boys_use,
  COALESCE(b.woman_number, 0) + COALESCE(b.girl_number, 0) + COALESCE(b.man_number, 0) + COALESCE(b.boy_number, 0) + COALESCE(a.men_regular_number, 0) + COALESCE(a.women_regular_number, 0) AS total_use
FROM
  my_cte AS a
FULL OUTER JOIN
  {{ref('data_passerbyuse_clean')}} AS b
ON
  a.facility = b.facility
AND a.date_auto = b.date_auto


  


 
 

 