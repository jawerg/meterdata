select id, ts
from {{ ref('interpolation_short_intervals') }}

intersect

select id, ts
from {{ ref('interpolation_long_intervals') }}
