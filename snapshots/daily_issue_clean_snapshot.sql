{% snapshot daily_issue_clean_snapshot %}

{{ config(
    target_schema="snapshots",
    unique_key="_id",
    strategy="timestamp",
    updated_at="date_auto"  
) }}

SELECT 
   _id::integer,
    full_facility,
    stalls,
    sides,
    facility,
    shift_type,
    category,
    date_auto,                             
    time_auto,                           
    issue,
    shutdown,
    full_partial,
    num_hours
FROM {{ ref('daily_issue_clean') }}

{% endsnapshot %}