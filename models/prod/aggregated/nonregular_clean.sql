
{{ config(
  materialized='table'
) }}


SELECT facility, date_auto, 
sum(child_girl_number :: integer) as girl_number,
sum(passerby_man_number :: integer) as man_number,
sum(passerby_woman_number :: integer) as woman_number,
sum(child_boy_number :: integer) as boy_number
FROM {{ref('nonregular_use_tracking_production')}}
group by date_auto, facility
order by date_auto desc
