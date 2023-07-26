{{config(order_by = ('id', 'dt'))}}

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
    case
       when minute(ts) = 0
           then greatest(ts + interval 1 hour, dt + interval 0 hour)
       when minute(ts) = 30
           then greatest(ts + interval 30 minute, dt + interval 0 hour)
    end   as ts_start,
    case
       when minute(ts) = 0
           then least(next_ts - interval 1 hour, dt + interval 24 hour)
       when minute(ts) = 30
           then least(next_ts - interval 30 minute, dt + interval 24 hour)
    end   as ts_end
from {{ ref('boundaries') }}
order by id, dt, ts_start
