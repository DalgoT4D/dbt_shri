{{ config(
  materialized='table'
) }}


SELECT 
    data->>'_id' AS id,
    data->>'_submitted_by' AS _submitted_by,
    data->>'starttime' AS starttime,
    attachment->>'id' AS attachment_id,
    attachment->>'xform' AS xform,
    attachment->>'filename' AS filename,
    attachment->>'instance' AS instance,
    attachment->>'mimetype' AS mimetype,
    attachment->>'download_url' AS download_url,
    attachment->>'download_large_url' AS download_large_url,
    attachment->>'download_small_url' AS download_small_url,
    attachment->>'download_medium_url' AS download_medium_url
FROM 
    (SELECT * FROM {{ source('source_shri_surveys', 'weeklyphoto') }}) AS s,
    LATERAL jsonb_array_elements(s.data->'_attachments') AS attachment
