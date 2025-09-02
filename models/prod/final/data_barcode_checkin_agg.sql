{{ config(
  materialized='table'
) }}

-- Aggregated staff barcode check-ins by facility with expected vs actual counts

WITH expected_checkins AS (
  SELECT 'Azad Nagar' AS facility, 6 AS expected_check_ins
  UNION ALL SELECT 'Basgoda', 5
  UNION ALL SELECT 'Dundibagh', 6
  UNION ALL SELECT 'Gomia', 5
  UNION ALL SELECT 'Jaridih CSR', 4
  UNION ALL SELECT 'Jaridih SBM', 4
  UNION ALL SELECT 'Kasmar', 4
  UNION ALL SELECT 'North Basgoda', 6
  UNION ALL SELECT 'Peterbaar', 4
  UNION ALL SELECT 'Katiya Bus Stand', 4
  UNION ALL SELECT 'Katiya Market', 4
  UNION ALL SELECT 'Patratu Block Campus', 4
  UNION ALL SELECT 'Patratu Railway Gate', 4
  UNION ALL SELECT 'Bela Museri', 6
  UNION ALL SELECT 'Bairo', 5
  UNION ALL SELECT 'Karanpur', 5
  UNION ALL SELECT 'Nemua', 6
  UNION ALL SELECT 'Vurahi', 5
  UNION ALL SELECT 'Mahuatand', 5
  UNION ALL SELECT 'Gomia Block', 5
  UNION ALL SELECT 'Dugdha Market', 5
  UNION ALL SELECT 'Dugdha Chhathghat', 5
),

-- Step 1: Add row numbers and time differences for deduplication
checkin_with_time_analysis AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY userid, facility, date_auto 
      ORDER BY time_auto ASC
    ) AS checkin_sequence,
    LAG(time_auto) OVER (
      PARTITION BY userid, facility, date_auto 
      ORDER BY time_auto ASC
    ) AS previous_checkin_time,
    EXTRACT(EPOCH FROM (
      time_auto - LAG(time_auto) OVER (
        PARTITION BY userid, facility, date_auto 
        ORDER BY time_auto ASC
      )
    )) / 60 AS minutes_since_last_checkin
  FROM {{ ref('data_barcode_checkin') }}
  WHERE position <> 'Unknown Position'
),

-- Step 2: Flag legitimate vs duplicate check-ins
deduplicated_checkins AS (
  SELECT 
    *,
    CASE 
      WHEN checkin_sequence = 1 THEN true  -- First check-in of the day is always legitimate
      WHEN minutes_since_last_checkin IS NULL THEN true  -- Handle NULL case (should be first check-in)
      WHEN minutes_since_last_checkin > 30 THEN true  -- Check-ins >30 minutes apart are legitimate
      ELSE false  -- Check-ins within 30 minutes are likely duplicates
    END AS is_legitimate_checkin
  FROM checkin_with_time_analysis
),

-- Step 3: Aggregate the deduplicated data
daily_checkins AS (
  SELECT 
    facility,
    date_auto,
    COUNT(DISTINCT userid) AS unique_staff_ids,
    COUNT(*) AS total_check_ins,
    COUNT(CASE WHEN is_legitimate_checkin = false THEN 1 END) AS duplicate_check_ins,
    COUNT(CASE WHEN is_legitimate_checkin = true THEN 1 END) AS legitimate_check_ins
  FROM deduplicated_checkins
  GROUP BY facility, date_auto
)

SELECT 
  dc.facility,
  dc.date_auto,
  COALESCE(ec.expected_check_ins, 0) AS expected_check_ins,
  dc.legitimate_check_ins,  -- Count of legitimate check-in events
  dc.unique_staff_ids,  -- Count of distinct staff positions
  dc.total_check_ins,   -- Total raw check-ins
  dc.duplicate_check_ins  -- Count of flagged duplicates for monitoring
FROM daily_checkins dc
JOIN expected_checkins ec ON dc.facility = ec.facility
ORDER BY dc.date_auto DESC, dc.facility