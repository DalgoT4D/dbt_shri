SELECT  
_id, 
_submitted_by,
timestamp_formatted::date as date_auto, 
(to_char(timestamp_formatted::time, 'HH:MI:SS'))::time AS time_auto,
facilityname as facility, 
_submission_time, 
shift_type,
null as category,
false as any_issue
FROM {{ref('daily_issue_form')}}
WHERE _id not in (select _id from {{ref('daily_issue_dashboard')}})