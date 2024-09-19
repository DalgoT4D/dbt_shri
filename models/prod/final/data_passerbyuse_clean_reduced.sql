
{{ config(
  materialized='table'
) }}


SELECT facility, date_auto, 
sum(coalesce(child_girl_number, 0)) as girl_number,
sum(coalesce(child_boy_number, 0)) as boy_number,
sum(coalesce(passerby_woman_number, 0)) as woman_number,
sum(coalesce(passerby_man_number, 0)) as man_number,
sum(CASE WHEN CAST(highlow AS INTEGER) = 1 THEN 1 ELSE 0 END) as high_use_count,  -- Cast to integer
sum(CASE WHEN CAST(highlow AS INTEGER) = 2 THEN 1 ELSE 0 END) as low_use_count  -- Cast to integer
FROM {{ref('data_passerbyuse_clean')}}
group by date_auto, facility
order by date_auto asc