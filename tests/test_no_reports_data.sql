SELECT *
FROM {{ ref('no_reports_data') }}
WHERE date > CURRENT_DATE
