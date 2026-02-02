{{ config(
  materialized='table'
) }}

-- Aggregated staff barcode check-ins by facility and position with expected vs actual counts

WITH position_mapping AS (
  SELECT position_raw, standardized_position
  FROM (VALUES
    ('Data Staff 1', 'Data Collector'),
    ('Data Staff 2', 'Data Collector'),
    ('Data Staff 3', 'Data Collector'),
    ('Data collection 1', 'Data Collector'),
    ('Data collection 2', 'Data Collector'),
    ('Data collection 3', 'Data Collector'),
    ('Data Collection 2', 'Data Collector'),
    ('Cleaning', 'Cleaning'),
    ('Cleaning Staff', 'Cleaning'),
    ('Night guard', 'Night Guard'),
    ('Night Guard', 'Night Guard'),
    ('Cleaning Backup/Night Guard', 'Night Guard'),
    ('Facility incharge', 'Facility Incharge'),
    ('Facility Incharge', 'Facility Incharge'),
    ('Facility incharge and water seller', 'Facility Incharge')
  ) AS mapping(position_raw, standardized_position)
),

expected_checkins_by_facility_position AS (
  -- Jharkhand facilities (corrected facility names)
  SELECT 'Dundibagh' AS facility, 'Data Collector' AS position_type, 2 AS expected_daily_checkins
  UNION ALL SELECT 'Dundibagh', 'Cleaning', 2 -- 1 cleaner * 2 checkins per day
  UNION ALL SELECT 'Dundibagh', 'Night Guard', 1
  UNION ALL SELECT 'Dundibagh', 'Facility Incharge', 1
  
  UNION ALL SELECT 'Basgoda', 'Data Collector', 2
  UNION ALL SELECT 'Basgoda', 'Cleaning', 2
  UNION ALL SELECT 'Basgoda', 'Facility Incharge', 1
  
  UNION ALL SELECT 'Gomia', 'Data Collector', 2
  UNION ALL SELECT 'Gomia', 'Cleaning', 2
  UNION ALL SELECT 'Gomia', 'Facility Incharge', 1
  
  UNION ALL SELECT 'Azad Nagar', 'Data Collector', 2
  UNION ALL SELECT 'Azad Nagar', 'Cleaning', 2
  UNION ALL SELECT 'Azad Nagar', 'Night Guard', 1
  
  UNION ALL SELECT 'North Basgoda', 'Data Collector', 2
  UNION ALL SELECT 'North Basgoda', 'Cleaning', 2
  UNION ALL SELECT 'North Basgoda', 'Night Guard', 1
  
  UNION ALL SELECT 'Jaridih CSR', 'Data Collector', 2
  UNION ALL SELECT 'Jaridih CSR', 'Cleaning', 2
  
  UNION ALL SELECT 'Jaridih SBM', 'Data Collector', 2
  UNION ALL SELECT 'Jaridih SBM', 'Cleaning', 2
  
  UNION ALL SELECT 'Kasmar', 'Data Collector', 2
  UNION ALL SELECT 'Kasmar', 'Cleaning', 2
  
  UNION ALL SELECT 'Peterbaar', 'Data Collector', 2
  UNION ALL SELECT 'Peterbaar', 'Cleaning', 2
  
  UNION ALL SELECT 'Mahuatand', 'Data Collector', 2
  UNION ALL SELECT 'Mahuatand', 'Cleaning', 2
  
  UNION ALL SELECT 'Gomia Block', 'Data Collector', 2
  UNION ALL SELECT 'Gomia Block', 'Cleaning', 2
  
  UNION ALL SELECT 'Dugdha Market', 'Data Collector', 2
  UNION ALL SELECT 'Dugdha Market', 'Cleaning', 2
  
  UNION ALL SELECT 'Dugdha Chhatghat', 'Data Collector', 2
  UNION ALL SELECT 'Dugdha Chhatghat', 'Cleaning', 2
  
  UNION ALL SELECT 'Katiya Bus Stand, Ramgarh', 'Data Collector', 2
  UNION ALL SELECT 'Katiya Bus Stand, Ramgarh', 'Cleaning', 2
  
  UNION ALL SELECT 'Katiya Market, Ramgarh', 'Data Collector', 2
  UNION ALL SELECT 'Katiya Market, Ramgarh', 'Cleaning', 2
  
  UNION ALL SELECT 'Patratu Block Campus, Ramgarh', 'Data Collector', 2
  UNION ALL SELECT 'Patratu Block Campus, Ramgarh', 'Cleaning', 2
  
  UNION ALL SELECT 'Patratu Rly Gate, Ramgarh', 'Data Collector', 2
  UNION ALL SELECT 'Patratu Rly Gate, Ramgarh', 'Cleaning', 2
  
  -- Bihar facilities
  UNION ALL SELECT 'Nemua', 'Data Collector', 2
  UNION ALL SELECT 'Nemua', 'Cleaning', 2
  UNION ALL SELECT 'Nemua', 'Night Guard', 1
  UNION ALL SELECT 'Nemua', 'Facility Incharge', 1
  
  UNION ALL SELECT 'Vurahi', 'Data Collector', 2
  UNION ALL SELECT 'Vurahi', 'Cleaning', 2
  UNION ALL SELECT 'Vurahi', 'Night Guard', 1
  
  UNION ALL SELECT 'Karanpur', 'Data Collector', 2
  UNION ALL SELECT 'Karanpur', 'Cleaning', 2
  UNION ALL SELECT 'Karanpur', 'Night Guard', 1
  
  UNION ALL SELECT 'Bairo', 'Data Collector', 2
  UNION ALL SELECT 'Bairo', 'Cleaning', 2
  UNION ALL SELECT 'Bairo', 'Night Guard', 1
  
  UNION ALL SELECT 'Bela Museri', 'Data Collector', 3
  UNION ALL SELECT 'Bela Museri', 'Cleaning', 2
  UNION ALL SELECT 'Bela Museri', 'Night Guard', 1
  
  UNION ALL SELECT 'Basgoda', 'Night Guard', 1
  UNION ALL SELECT 'Bahadurpur', 'Data Collector', 2
  UNION ALL SELECT 'Urwan, Koderma', 'Data Collector', 2
  UNION ALL SELECT 'PM SHRI School Basgoda', 'Data Collector', 1
  UNION ALL SELECT 'High School Dugdha', 'Cleaning', 2
  UNION ALL SELECT 'Bahadurpur', 'Cleaning', 2
  UNION ALL SELECT 'High School Dugdha', 'Data Collector', 1
  UNION ALL SELECT 'Urwan, Koderma', 'Cleaning', 2
  UNION ALL SELECT 'PM SHRI School Basgoda', 'Cleaning', 2
),

-- Process check-ins with standardized positions
checkins_with_standard_positions AS (
  SELECT 
    c.*,
    COALESCE(pm.standardized_position, 
      CASE WHEN c.position = 'Unknown Position' THEN 'Unknown Position' 
           ELSE 'Other' END
    ) AS standardized_position
  FROM {{ ref('data_barcode_checkin') }} c
  LEFT JOIN position_mapping pm ON c.position = pm.position_raw
),

-- Aggregate by facility, date, and position type
daily_checkins_by_position AS (
  SELECT 
    facility,
    date_auto,
    standardized_position,
    COUNT(*) AS actual_checkins
  FROM checkins_with_standard_positions
  WHERE date_auto IS NOT NULL
  GROUP BY facility, date_auto, standardized_position
),

-- Calculate totals and differences
final_aggregation AS (
  SELECT 
    dc.facility,
    dc.date_auto,
    dc.standardized_position,
    COALESCE(ec.expected_daily_checkins, 0) AS expected_checkins,
    dc.actual_checkins,
    GREATEST(0, dc.actual_checkins - COALESCE(ec.expected_daily_checkins, 0)) AS duplicate_checkins,
    GREATEST(0, COALESCE(ec.expected_daily_checkins, 0) - dc.actual_checkins) AS missing_checkins
  FROM daily_checkins_by_position dc
  LEFT JOIN expected_checkins_by_facility_position ec 
    ON dc.facility = ec.facility 
    AND dc.standardized_position = ec.position_type
)

SELECT 
  facility,
  date_auto,
  standardized_position AS position_type,
  expected_checkins,
  actual_checkins,
  duplicate_checkins,
  missing_checkins
FROM final_aggregation
ORDER BY date_auto DESC, facility, position_type