{{ config(
   indexes=[
      {'columns': ['_airbyte_raw_id'], 'type': 'hash'}
    ],
    materialized='table'
) }}

WITH flattened AS (
    SELECT * FROM (
        {{
            flatten_json(
                model_name = source('source_shri_surveys', 'facility_koboid_link'),
                json_column = 'data'
            )
        }}
    ) AS derived_flattened  -- avoid raw unaliased subquery
),

deduplicated AS (
    {{ dbt_utils.deduplicate(
        relation='flattened',
        partition_by='_id',
        order_by='_id DESC'
    ) }}
)

SELECT
    _airbyte_raw_id,
    _id,
    CASE
        -- Correct the Kobo username typo so Sayal North submissions join downstream models.
        WHEN facilityname = 'Sayal North Patratu'
            AND kobo_username = 'savalnorthratratu'
            THEN 'sayalnorthpatratu'
        ELSE kobo_username
    END AS kobo_username,
    start,
    starttime,
    meta_deprecatedid,
    endtime,
    _validation_status,
    "end",
    formhub_uuid,
    meta_instancename,
    _uuid,
    _xform_id_string,
    _tags,
    _submission_time,
    meta_rootuuid,
    _geolocation,
    facilityname,
    _status,
    meta_instanceid,
    _attachments,
    _submitted_by,
    _notes,
    __version__
FROM deduplicated
