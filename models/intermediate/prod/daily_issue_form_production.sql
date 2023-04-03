{{ config(
  materialized='table',
    schema='final'

) }}


with merged_normalized AS
       (SELECT "facilityname" as "facility", "shift_type",
       unnest(array['cleanliness_group_outage_pan_full', 
                     'electrical_group_outage_wiring_hours',
                     'plumbing_group_outage_pipe_full', 
                     'plumbing_group_outage_tools',
                     'plumbing_group_outage_socket', 
                     'electrical_group_outage_boring_hours',
                     'infrastructure_group_outage_wall_hours',
                     'cleanliness_group_outage_drainblock_hours',
                     'technology_group_outage_internet',
                     'plumbing_group_outage_pipe_full',
                     'cleanliness_group_outage_basinclean',
                     'cleanliness_group_outage_odor_hours',
                     'electrical_group_outage_boring_full',
                     'electrical_group_outage_wiring_full',
                     'supplies_group_outage_soap_hours'
       ]) AS issue,
       unnest(array[cleanliness_group_outage_pan_full as , 
                     electrical_group_outage_wiring_hours,
                     plumbing_group_outage_pipe_full, 
                     plumbing_group_outage_tools,
                     plumbing_group_outage_socket, 
                     electrical_group_outage_boring_hours,
                     infrastructure_group_outage_wall_hours,
                     cleanliness_group_outage_drainblock_hours,
                     technology_group_outage_internet,
                     plumbing_group_outage_pipe_full,
                     cleanliness_group_outage_basinclean,
                     cleanliness_group_outage_odor_hours,
                     electrical_group_outage_boring_full,
                     electrical_group_outage_wiring_full,
                     supplies_group_outage_soap_hours]) AS score
FROM {{ref('daily_issue_form')}}
)



select *,
        case 
            when score = '0' then 'NO' 
            when score = '1' then 'YES'
        END as shutdown
from  merged_normalized
where score is NOT NULL