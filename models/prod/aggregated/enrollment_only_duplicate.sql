
SELECT *
FROM {{ ref('enrollment_aggregated') }}
WHERE userid IN (
  SELECT userid
  FROM {{ ref('enrollment_aggregated') }}
  GROUP BY userid
  HAVING COUNT(*) > 1
)
ORDER BY userid
