{% macro gap_fill_bounds(ts, next_ts) -%}
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
                    then greatest(ts + interval 1 hour, dt + interval 0 hour)
               when minute(ts) = 30
                   then greatest(ts + interval 30 minute, dt + interval 0 hour)
            end
        else makeDateTime(year(dt), month(dt), day(dt), 0, 0, 0, 'UTC')
    end as start_ts,
    case
       when minute(next_ts) = 0
           then least(next_ts - interval 1 hour, dt + interval 24 hour)
       when minute(next_ts) = 30
           then least(next_ts - interval 30 minute, dt + interval 24 hour)
    end as end_ts
{%- endmacro %}
