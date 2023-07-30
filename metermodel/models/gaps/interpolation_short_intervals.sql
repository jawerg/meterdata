{{config(order_by = ('id', 'ts'))}}

with
gap_closer as (
    select
        id,
        ts + interval 30 minute as shifted_ts,
        (ec + neighbor(ec, 1)) / 2 as interpolated_ec
    from {{ ref('interpolation_long_intervals_and_bounds') }}
    where minute(ts) = 0
      and id = neighbor(id, 1)
      and gap_id = neighbor(gap_id, 1) -- gaps could be subsequent, leading to duplicates.
      and interpolated_ec is not null
)
select id, shifted_ts as ts, interpolated_ec as ec
from gap_closer
where (id, shifted_ts) not in (
    select id, ts
    from meterdata_gaps.interpolation_long_interval_bounds
)
order by id, ts
