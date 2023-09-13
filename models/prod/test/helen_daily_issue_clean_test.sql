select 

_id,
facility,
shift_type,
category,
date_auto,
time_auto,
issue,
shutdown,
full_partial,
num_hours

from {{ source('source_shri_surveys', 'dailyissuetest') }}