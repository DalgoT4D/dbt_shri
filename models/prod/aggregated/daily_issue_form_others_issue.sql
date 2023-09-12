select  
       _id,
       _submitted_by,
       _submission_time,
       facilityname as facility,
       to_date(timestamp_formatted, 'YYYY-MM-DD') AS date_auto,
       date_trunc('minute', timestamp_formatted::timestamp)::time AS time_auto,
       minorissue_type,
       shift_type,
       'Others' as category,
       other_group_other_fixed as issue,
       null as fixed,
       true as any_issue,
       case other_group_outage_other_full
          When '1' THEN 'part day'
          When '2' THEN 'full day'
          ELSE other_group_outage_other_full
        End as full_partial,
       other_group_outage_other_hours as num_hours,
       CASE other_group_outage_other
         WHEN '0' THEN 'no'
         WHEN '1' THEN 'yes'
         ELSE other_group_outage_other
       END as shutdown


FROM {{ref('daily_issue_form')}}  where other_group_other_fixed is not null