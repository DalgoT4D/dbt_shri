 SELECT facility, _id, issue, shutdown, full_partial, num_hours, time_auto
 FROM prod_final.daily_issue_clean
 where date_auto < '2023-08-31' and facility = 'Dundibagh'