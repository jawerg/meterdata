with

gap_closer as (
    select
        id,
        ts + interval 30 minute as shifted_ts,
        (ec + neighbor(ec, 1)) / 2 as ec
    from {{ ref('clean_source_data') }}
    where minute(ts) = 0
      and id = neighbor(id, 1)
      and ts - neighbor(ts, 1) = 60 * 60 -- every larger gab is covered by interpolation.
)

select id, shifted_ts as ts, ec
from gap_closer
