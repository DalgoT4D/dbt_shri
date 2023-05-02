
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
 coalesce(b.woman_number, 0) as woman_number, 
 coalesce(b.girl_number, 0) as girl_number, 
 coalesce(b.man_number, 0) as man_number, 
 coalesce(b.boy_number, 0) as boy_number,
 a.men_regular_number + coalesce(b.man_number, 0) as men_use,
 a.women_regular_number + coalesce(b.woman_number, 0) as women_use,
 coalesce(b.boy_number, 0) as boys_use,
 coalesce(b.girl_number, 0) as girls_use,
 coalesce(b.woman_number, 0) + coalesce(b.girl_number, 0) + coalesce(b.man_number,0 ) + coalesce(b.boy_number, 0) + a.men_regular_number + a.women_regular_number  as total_use
 
 from my_cte as a
 left join {{ref('data_passerbyuse_clean')}} as b
 on a.facility = b.facility and a.date_auto = b.date_auto 
 