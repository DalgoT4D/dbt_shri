{{ config(
  materialized='table'
) }}

-- {{ref('daily_issue_form')}} -> this is referring to daily_issue_form table which is a normalized table 

-- The WITH clause defines a common table expression (CTE) called my_cte that selects columns from 
-- the daily_issue_form and facility_table tables and unnests some arrays to create new rows for 
-- each element in the arrays. The WHERE clause filters out rows where the minorissue_type column 
-- contains certain values.


with my_cte AS (SELECT _id, _submitted_by, minorissue_type, facilityname as facility, shift_type, timestamp_formatted as dateauto,
       unnest(array['Electrical - bulb', 
                    'Electrical - wiring',
                    'Electrical - boring',
                    'Plumbing - basin tap', 
                    'Plumbing - toilet tap',
                    'Plumbing - pipe',
                    'Technology - tablet',
                    'Technology - Internet', 
                    'Supplies - harpic',
                    'Supplies - soap',
                    'Infrastructure - gate'
       
       ]) AS issue,
       unnest(array[electrical_group_outage_bulb, 
                    electrical_group_outage_wiring, 
                    electrical_group_outage_boring,
                    plumbing_group_outage_basintap,
                    plumbing_group_outage_toilettap, 
                    plumbing_group_outage_pipe,
                    technology_group_outage_tablet, 
                    technology_group_outage_internet,
                    supplies_group_outage_harpicetc,
                    supplies_group_outage_soap, 
                    infrastructure_group_outage_gate
                    ]) AS outage, 

       unnest(array[electrical_group_bulb_fixed, 
                    electrical_group_wiring_fixed,
                    electrical_group_boring_fixed,
                    plumbing_group_basintap_fixed, 
                    plumbing_group_toilettap_fixed,
                    plumbing_group_pipe_fixed,
                    technology_group_tablet_fixed, 
                    technology_group_internet_fixed,  
                    supplies_group_harpicetc_fixed, 
                    supplies_group_soap_fixed,
                    infrastructure_group_gate_fixed]) AS fixed, 

       unnest(array[electrical_group_outage_bulb_full, 
                    electrical_group_outage_wiring_full,
                    electrical_group_outage_boring_full,
                    plumbing_group_outage_basintap_full, 
                    plumbing_group_outage_toilettap_full,
                    plumbing_group_outage_pipe_full,
                    technology_group_outage_tablet_full, 
                    technology_group_outage_internet_full,
                    supplies_group_outage_harpicetc_full,
                    supplies_group_outage_soap_full,
                    infrastructure_group_outage_gate_full]) AS full_part,

       unnest(array[electrical_group_outage_bulb_hours, 
                    electrical_group_outage_wiring_hours,
                    electrical_group_outage_boring_hours,
                    plumbing_group_outage_basintap_hours, 
                    plumbing_group_outage_toilettap_hours, 
                    plumbing_group_outage_pipe_hours,
                    technology_group_outage_internet_hours, 
                    technology_group_outage_tablet_hours,
                    supplies_group_outage_harpicetc_hours,
                    supplies_group_outage_soap_hours,
                    infrastructure_group_outage_gate_hours]) AS num_hours
      
       FROM {{ref('daily_issue_form')}}
       WHERE NOT ((minorissue_type LIKE '%1%') 
             AND (minorissue_type LIKE '%2%') 
             AND (minorissue_type LIKE '%3%') 
             AND (minorissue_type LIKE '%4%') 
             AND (minorissue_type LIKE '%5%') 
             AND (minorissue_type LIKE '%6%') 
             AND (minorissue_type LIKE '%7%')
))



SELECT 
       _id,
       facility,
       _submitted_by,
       to_date(dateauto, 'YYYY-MM-DD') AS date_auto,
       minorissue_type,
       null as subcategory,
       shift_type,
       issue,
       fixed,
       true as any_issue,
       case full_part 
          When '1' THEN 'part day'
          When '2' THEN 'full day'
        End as full_partial,
       num_hours,
       CASE outage
         WHEN '0' THEN 'NO'
         WHEN '1' THEN 'YES'
         ELSE outage
       END as shutdown
FROM my_cte 
where not (outage is null and
fixed is null and
full_part is null and
num_hours is null)
order by date_auto