{{config(order_by=('dt', 'ts'))}}


with years as (
    select distinct year(ts) as year
    from {{ source('meterdata', 'meter_halfhourly_dataset') }}
)
select
    makeDate(years.year, month(ts), day(ts)) as dt,
    makeDateTime(
        years.year, month(ts), day(ts),
        hour(ts), 0, 0,
        'UTC'
    ) as ts,
    avg(val) as val
from {{ source('meterdata', 'meter_halfhourly_dataset') }}
     cross join years
group by dt, ts
order by dt, ts
