with meterdata as (
    select id, ts, val
    from meterdata.target
    where id = 'MAC005558'

    union all

    select id, ts + interval 30 minute as ts, (val + neighbor(val, 1)) / 2 as val
    from meterdata.target
    where id = 'MAC005558'
      and neighbor(ts, 1) - ts = 60 * 60
      and neighbor(val, 1) is not null
)
select * from meterdata order by id, ts;
