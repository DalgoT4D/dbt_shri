{{ config(
  materialized='table'
) }}

-- Missing check-ins data showing days when actual checkins were less than expected
-- Based on data_barcode_checkin_agg upstream model

SELECT 
  date_auto AS date,
  facility,
  position_type AS position,
  expected_checkins,
  actual_checkins
FROM {{ ref('data_barcode_checkin_agg') }}
WHERE 
  missing_checkins > 0  -- Only include rows where actual < expected
ORDER BY date DESC, facility, position