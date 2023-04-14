{{ config(
  materialized='table',
  schema='aggregated'
) }}


SELECT userid, COUNT(userid)
FROM {{ ref('enrollment_production') }}
GROUP BY userid
HAVING COUNT(userid) > 1

