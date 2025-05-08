{{ config(
  materialized='table'
) }}


select  
    _id,
    _submitted_by,
    _submission_time,
    facilityname as facility,
    date_auto,
    time_auto,
    minorissue_type,
    shift_type,
    'Other' as category,
    other_group_other_fixed as issue,
    null as fixed,
    null as full_facility,
    null as stalls,
    null as sides,
    true as any_issue,
    case other_group_outage_other_full
        when '1' then 'part day'
        when '2' then 'full day'
        else other_group_outage_other_full
    end as full_partial,
    other_group_outage_other_hours as num_hours,
    case other_group_outage_other
        when '0' then 'no'
        when '1' then 'yes'
        else other_group_outage_other
    end as shutdown

from {{ ref('daily_issue_form') }}
where other_group_other_fixed is not null
