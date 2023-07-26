{{config(order_by = ('id', 'dt'))}}

/*
    In order to reduce the amount of data joined here, we will filter on the date of
    a gap. However, as some gaps might spread over several dates, the bounds will be
    unnested in case they span multiple days.
    A new row either starts/ends with the actual end of the gap or 00:00.

 */

select
    id,
    {{ gap_fill_bounds(ts, next_ts) }}
from {{ ref('boundaries') }}
order by id, dt, start_ts
