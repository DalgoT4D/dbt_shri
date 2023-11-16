select begin_group_waterquality_watertimes,
       begin_group_waterquality_waterquality,
       starttime,
       _submitted_by 
from {{ ref('waterquality_normalized') }} 
