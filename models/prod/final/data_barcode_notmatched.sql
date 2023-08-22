{{ config(
  materialized='table'
) }}

select
userid, 
position, 
facility, 
date_auto, 
TO_TIMESTAMP(datetime_auto_day, 'YYYY-MM-DD HH24:MI:SS')::TIME as time_auto
FROM {{ref('staff_barcode')}} where position is null
order by date_auto