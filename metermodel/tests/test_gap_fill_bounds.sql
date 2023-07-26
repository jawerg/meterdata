select
    id,
    {{ gap_fill_bounds(ts, next_ts) }}
from {{ ref('gap_fill_bounds')}}

except

select id, dt, start_ts, end_ts
from {{ ref('gap_fill_bounds_results')}}
