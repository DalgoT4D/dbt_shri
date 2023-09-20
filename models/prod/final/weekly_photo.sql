{{ config(
  materialized='table'
) }}

select
a.id,
a.attachment_id,
a.download_url as image_link,
a.starttime::timestamp with time zone::date AS date_auto,
b.facilityname as facility
from {{ref('weekly_photo_normalized')}} as a
LEFT JOIN {{ ref('facility_koboid_link_normalized') }} AS b
ON a._submitted_by = b.kobo_username