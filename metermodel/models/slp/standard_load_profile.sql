{{config(order_by=('dt', 'ts'))}}


with years as (
    select distinct year(ts) as year
    from raw.meter_halfhourly_dataset_base
)
select
    makeDate(
        years.year,
        month(ts),
        day(ts)
    ) as dt,
    makeDateTime(
        years.year,
        month(ts),
        day(ts),
        hour(ts),
        0,
        0,
        'UTC'
    ) as ts,
    avg(val) as val
from raw.meter_halfhourly_dataset_base
     cross join years
group by dt, ts
order by dt, ts
