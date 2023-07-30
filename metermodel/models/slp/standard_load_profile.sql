{{config(order_by=('dt', 'ts'))}}


with years as (
    select distinct year(ts) as year
    from {{ ref('clean_and_partitioned_source_data') }}
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
    avg(ec) as ec
from {{ ref('clean_and_partitioned_source_data') }}
     cross join years
group by dt, ts
order by dt, ts
