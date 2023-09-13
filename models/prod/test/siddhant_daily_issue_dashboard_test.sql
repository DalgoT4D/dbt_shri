select 

    facility,
    date_auto,
    COALESCE(category, '') AS category,
    COALESCE(issue, '') AS issue,
    COALESCE(shutdown, '') AS shutdown,
    COALESCE(full_partial, '') AS full_partial,
    COALESCE(num_hours, '') AS num_hours

from {{ref('daily_issue_dashboard')}}

