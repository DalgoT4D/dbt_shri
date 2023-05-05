{{ config(
  materialized='table',
  schema='aggregated'
) }}


SELECT *
FROM {{ ref('enrollment_production') }}
WHERE userid IN (
  SELECT userid
  FROM {{ ref('enrollment_production') }}
  GROUP BY userid
  HAVING COUNT(*) > 1
)

