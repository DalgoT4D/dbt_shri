SELECT 
    _id,
    facility,
    COALESCE(shift_type, '') AS shift_type,
    COALESCE(category, '') AS category,
    date_auto,
    time_auto,
    COALESCE(issue, '') AS issue,
    COALESCE(shutdown, '') AS shutdown,
    COALESCE(full_partial, '') AS full_partial,
    COALESCE(num_hours, '') AS num_hours
FROM {{ ref('daily_issue_clean') }}