{{ config(
  materialized='table'
) }}

select date_auto,
       to_char(date_trunc('minute', time_auto), 'HH24:MI') AS time_auto,
       facility

from prod_final.data_barcode_checkin where position = 'Cleaning'