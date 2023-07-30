{{
    config(
        partition_by='halfMD5(id) % 64',
        order_by=('id', 'ts')
    )
}}

with

gap_closer as (
    select
        id,
        ts + interval 30 minute as shifted_ts,
        (ec + neighbor(ec, 1)) / 2 as ec
    from {{ ref('clean_and_partitioned_source_data') }}
    where minute(ts) = 0
      and id = neighbor(id, 1)
      and ts - neighbor(ts, 1) = 60 * 60 -- every larger gab is covered by interpolation.
)

select id, shifted_ts as ts, ec
from gap_closer
order by id, ts
settings optimize_read_in_order = 0
