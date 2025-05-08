{% snapshot facility_submission_snapshot %}

{{ config(
    target_schema="snapshots",
    unique_key="date",
    strategy="timestamp",
    updated_at="date"  
) }}

    SELECT 
        facility, 
        date, 
        submission_count
    FROM {{ ref('no_reports_data') }}

{% endsnapshot %}
