{{config(order_by = ('id', 'gap_id', 'dt'))}}

/*
    In order to reduce the amount of data joined here, we will filter on the date of
    a gap. However, as some gaps might spread over several dates, the bounds will be
    unnested in case they span multiple days.
    A new row either starts/ends with the actual end of the gap or 00:00.

 */

select
    id,
    gap_id,
    arrayJoin(
        arrayMap(
            x -> toDate(x, 'UTC'),
            range(toUInt32(ts), toUInt32(next_ts), 24 * 60 * 60)
        )
    ) as dt,
    case
        when dt == ts::Date
            then greatest(ts, dt + interval 0 hour)
        else makeDateTime(year(dt), month(dt), day(dt), 0, 0, 0, 'UTC')
    end as start_ts,
    case
        when dt != next_ts::Date
            then  makeDateTime(year(dt), month(dt), day(dt), 23, 0, 0, 'UTC')
        else least(next_ts, dt + interval 23 hour)
    end as end_ts
from {{ ref('boundaries') }}
order by id, dt, start_ts
