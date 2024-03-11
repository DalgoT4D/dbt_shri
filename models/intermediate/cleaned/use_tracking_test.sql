{{ config(
  materialized='table'
) }}


with cte as (select  _id, to_date
            ( coalesce ( 
            begin_group_ajqop6jqs_name_timestamp_formatted, 
            begin_group_kthfwkunp_name_timestamp_formatted, 
            begin_group_5xw8upowl_name_timestamp_formatted, 
            name_timestamp_formatted), 'YYYY-MM-DD') as date_auto,
            _submitted_by,
            numberid
            
         from {{ ref('use_tracking_normalized') }} )
            
select * from cte where date_auto = '2021-11-07' and _submitted_by = 'shriindia'