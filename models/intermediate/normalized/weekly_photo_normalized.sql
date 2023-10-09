WITH expanded AS (
    SELECT 
        _airbyte_data::json->>'_id' AS id,
        _airbyte_data::json->'data'->>'_submitted_by' AS _submitted_by,
        _airbyte_data::json->'data'->>'starttime' AS starttime,
        json_array_elements_text(_airbyte_data::json->'data'->'_attachments')::json AS attachment
    FROM {{source('source_shri_surveys', 'weeklyphoto')}}
       
)

SELECT 
    id,
    _submitted_by,
    starttime,
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
    expanded


