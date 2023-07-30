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
            x -> toDate(x),
            range(toUInt32(ts), toUInt32(next_ts), 24 * 60 * 60)
        )
    ) as dt,
    case
        when dt == ts::Date
            then case
                when minute(ts) = 0
                    then greatest(ts + interval 1 hour, dt + interval 1 hour)
               when minute(ts) = 30
                   then greatest(ts + interval 30 minute, dt + interval 1 hour)
            end
        else makeDateTime(year(dt), month(dt), day(dt), 0, 0, 0, 'UTC')
    end as start_ts,
    case
        when dt != next_ts::Date
            then  makeDateTime(year(dt), month(dt), day(dt), 23, 0, 0, 'UTC')
        else case
           when minute(next_ts) = 0
               then least(next_ts - interval 1 hour, dt + interval 1 day)
           when minute(next_ts) = 30
               then least(next_ts - interval 30 minute, dt + interval 1 day)
        end
    end as end_ts
from {{ ref('boundaries') }}
order by id, dt, start_ts
