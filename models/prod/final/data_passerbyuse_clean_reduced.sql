{{ config(
  materialized='table'
) }}


SELECT
    facility,
    date_auto, 
    sum(coalesce(child_girl_number, 0)) AS girl_number,
    sum(coalesce(child_boy_number, 0)) AS boy_number,
    sum(coalesce(passerby_woman_number, 0)) AS woman_number,
    sum(coalesce(passerby_man_number, 0)) AS man_number,
    CASE 
        WHEN sum(CASE WHEN cast(highlow AS INTEGER) = 1 THEN 1 ELSE 0 END) > sum(CASE WHEN cast(highlow AS INTEGER) = 2 THEN 1 ELSE 0 END)
            THEN 'high'
        WHEN sum(CASE WHEN cast(highlow AS INTEGER) = 2 THEN 1 ELSE 0 END) > sum(CASE WHEN cast(highlow AS INTEGER) = 1 THEN 1 ELSE 0 END)
            THEN 'low'
    END AS highlow_usage_level 
FROM {{ ref('data_passerbyuse_clean') }}
GROUP BY date_auto, facility
ORDER BY date_auto ASC
