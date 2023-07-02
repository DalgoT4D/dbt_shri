
{{ config(
  materialized='table',
  schema='final'
) }}


SELECT facility, date_auto, 
sum(coalesce(child_girl_number, 0)) as girl_number,
sum(coalesce(child_boy_number, 0)) as boy_number,
sum(coalesce(passerby_woman_number, 0)) as woman_number,
sum(coalesce(passerby_man_number, 0)) as man_number
FROM {{ref('data_passerbyuse_clean')}}
group by date_auto, facility
order by date_auto asc
