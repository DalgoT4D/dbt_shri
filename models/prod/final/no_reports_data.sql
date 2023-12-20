{{ config(
  materialized='table'
) }}


SELECT 
    date_auto,
    facility,
    COUNT(*) AS row_count
FROM 
    {{ref('daily_issue_clean')}}
GROUP BY 
    date_auto,
    facility
HAVING 
    COUNT(*) = 0 OR COUNT(*) = 1
ORDER BY
    facility, 
    date_auto
