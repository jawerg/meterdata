{{config(order_by = ('id', 'dt', 'ts_start', 'ts_end'))}}

/*
    In order to reduce the amount of data joined here, we will filter on the date of
    a gap. However, as some gaps might spread over several dates, the bounds will be
    unnested in case they span multiple days.
    A new row either starts/ends with the actual end of the gap or 00:00.

 */

select
  id,
  arrayJoin(
    arrayMap(
      x -> toDate(x),
        range(toUInt32(ts), toUInt32(next_ts), 24 * 60 * 60)
    )
  ) as dt,
  greatest(ts + interval 1 hour, dt + interval 0 hour) as ts_start,
  least(next_ts - interval 1 hour, dt + interval 24 hour) as ts_end
from {{ ref('boundaries') }}
order by id, dt, ts_start
