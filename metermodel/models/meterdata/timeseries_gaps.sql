{{config(order_by = ('id', 'dt', 'ts_start', 'ts_end'))}}


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
from {{ ref('timeseries_gap_bounds') }}
order by id, dt, ts_start
