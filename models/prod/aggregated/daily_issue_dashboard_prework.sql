SELECT 
       _id,
       facility,
       _submitted_by,
       _submission_time,
       to_date(dateauto, 'YYYY-MM-DD') AS date_auto,
       timeauto AS time_auto,
       minorissue_type,
       category,
       shift_type,
       issue,
       fixed,
       true as any_issue,
       CASE full_part 
          WHEN '1' THEN 'part day'
          WHEN '2' THEN 'full day'
        END as full_partial,
       num_hours,
       CASE
          WHEN outage IS NULL THEN 'no'
          WHEN outage = '0' THEN 'no'
          WHEN outage = '1' THEN 'yes'
       ELSE outage
       END as shutdown
FROM {{ref('daily_issue_form_aggregate')}}
where fixed is not null
order by date_auto