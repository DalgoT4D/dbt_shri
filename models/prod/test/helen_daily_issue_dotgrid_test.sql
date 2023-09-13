select facility, date_auto, status from {{ source('source_shri_surveys', 'dailyissuedotgridtest') }}

