{{config(order_by = ('dt', 'ts'))}}


with years as (
    select distinct year(tstp) as year
    from {{ source('meterdata', 'meter_halfhourly_dataset') }}
)
select
    makeDate(
        years.year,
        month(tstp),
        day(tstp)
    ) as dt,
    makeDateTime(
        years.year,
        month(tstp),
        day(tstp),
        hour(tstp),
        0,
        0
    ) as ts,
    avg(`energy(kWh/hh)`) as ec
from {{ source('meterdata', 'meter_halfhourly_dataset') }}
     cross join years
group by dt, ts
order by dt, ts
