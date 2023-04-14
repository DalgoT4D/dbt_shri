
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

 select a.*,
 b.woman_number, 
 b.girl_number, 
 b.man_number, 
 b.boy_number,
 a.men_regular_number + b.man_number as men_use,
 a.women_regular_number + b.woman_number as women_use,
 b.boy_number as boys_use,
 b.girl_number as girls_use,
 b.woman_number + b.girl_number + b.man_number + b.boy_number + a.men_regular_number + a.women_regular_number  as total_use
 
 from my_cte as a
 left join {{ref('nonregular_clean')}} as b
 on a.facility = b.facility and a.date_auto = b.date_auto 
