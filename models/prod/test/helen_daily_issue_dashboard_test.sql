select 

    facility,
    date_auto,
    category,
    issue,
    shutdown,
    full_partial,
    num_hours

from {{ source('source_shri_surveys', 'dailyissuedashboardtest') }}

