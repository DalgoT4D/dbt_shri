SELECT *
FROM {{ ref('uses_no_report') }}
WHERE date_auto > CURRENT_DATE
