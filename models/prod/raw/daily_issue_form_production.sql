{{ config(
  materialized='table'
) }}


with my_cte AS (SELECT _id, minorissue_type, facilityname as facility, shift_type, timestamp_formatted as dateauto,
       unnest(array['Electrical - bulb', 
                    'Plumbing - basin tap', 
                    'Technology - Internet', 
                    'Electrical - wiring',
                    'Electrical - boring',
                    'Plumbing - toilet tap',
                    'Technology - tablet',
                    'Supplies - harpic',
                    'Infrastructure - gate',
                    'Supplies - soap',
                    'Plumbing - pipe'
       
       ]) AS issue,
       unnest(array[electrical_group_outage_bulb, 
                    plumbing_group_outage_basintap, 
                    technology_group_outage_internet, 
                    electrical_group_outage_wiring, 
                    electrical_group_outage_boring,
                    plumbing_group_outage_toilettap,
                    technology_group_outage_tablet, 
                    supplies_group_outage_harpicetc,
                    infrastructure_group_outage_gate,
                    supplies_group_outage_soap, 
                    plumbing_group_outage_pipe]) AS outage, 
       unnest(array[electrical_group_bulb_fixed, 
                    plumbing_group_basintap_fixed, 
                    technology_group_internet_fixed, 
                    electrical_group_wiring_fixed,
                    electrical_group_boring_fixed,
                    plumbing_group_toilettap_fixed, 
                    technology_group_tablet_fixed, 
                    supplies_group_harpicetc_fixed, 
                    infrastructure_group_gate_fixed,
                    supplies_group_soap_fixed,
                    plumbing_group_pipe_fixed]) AS fixed, 
       unnest(array[electrical_group_outage_bulb_full, 
                    plumbing_group_outage_basintap_full, 
                    technology_group_outage_internet_full, 
                    electrical_group_outage_wiring_full,
                    electrical_group_outage_boring_full,
                    plumbing_group_outage_toilettap_full,
                    technology_group_outage_tablet_full, 
                    supplies_group_outage_harpicetc_full,
                    infrastructure_group_outage_gate_full,
                    supplies_group_outage_soap_full,
                    plumbing_group_outage_pipe_full]) AS full_part,
       unnest(array[electrical_group_outage_bulb_hours, 
                    plumbing_group_outage_basintap_hours, 
                    technology_group_outage_internet_hours, 
                    electrical_group_outage_wiring_hours,
                    electrical_group_outage_boring_hours,
                    plumbing_group_outage_toilettap_hours, 
                    technology_group_outage_tablet_hours,
                    supplies_group_outage_harpicetc_hours,
                    infrastructure_group_outage_gate_hours,
                    supplies_group_outage_soap_hours,
                    plumbing_group_outage_pipe_hours]) AS num_hours
       FROM {{ref('daily_issue_form')}}
       where minorissue_type is not null
)



SELECT facility,
       shift_type,
       issue,
       fixed,
       _id, 
       case full_part 
          When '1' THEN 'part day'
          When '2' THEN 'full day'
        End as full_partial,
       num_hours,
       CASE outage
         WHEN '0' THEN 'NO'
         WHEN '1' THEN 'YES'
         ELSE outage
       END as shutdown,
       to_date(dateauto, 'YYYY-MM-DD') AS date_auto
FROM my_cte 
where outage is not null 
