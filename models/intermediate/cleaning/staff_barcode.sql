{{ config(
  materialized='table',
    schema='intermediate'

) }}

with cte as(  
  select
    maintenance_numberid_in as userid,
    coalesce(group_qx5fr96_name_timestamp_formatted, begin_group_chvgkvrk8_name_timestamp_formatted) as name_timestamp_formatted,
    {{ dbt_utils.star(from = ref('staff_barcode_normalized'), except=['begin_group_chvgkvrk8_name_timestamp_formatted', 'group_qx5fr96_name_timestamp_formatted', '_airbyte_ab_id', '_id', 'start', 'starttime', '_validation_status', 'formhub_uuid', 'maintenance_numberid_in', '_uuid', '_xform_id_string', '_tags', '_geolocation', '_status', 'meta_instanceid', '_attachments', '_notes', 'begin_group_chvgkvrk8_name_timestamp', '__version__', 'group_qx5fr96_name_timestamp'])}}
    
  from {{ ref('staff_barcode_normalized') }} 
)

select
    a.*,
    b.facilityname as facility
from cte as a left join {{ ref('facility_koboid_link_normalized') }} as b on
a._submitted_by = b.kobo_username