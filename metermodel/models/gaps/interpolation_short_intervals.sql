{{config(order_by = ('id', 'ts'))}}

with
gap_closer as (
    select
        id,
        ts + interval 30 minute as shifted_ts,
        (ec + neighbor(ec, 1)) / 2 as ec
    from {{ ref('interpolation_long_intervals_and_bounds') }}
    where minute(ts) = 0
      and id = neighbor(id, 1)
      and gap_id = neighbor(gap_id, 1) -- gaps could be subsequent, leading to duplicates.
)
select id, shifted_ts as ts, ec
from gap_closer
where (id, shifted_ts) not in (
        select id, ts
        from {{ ref('interpolation_long_interval_bounds') }}
    )
