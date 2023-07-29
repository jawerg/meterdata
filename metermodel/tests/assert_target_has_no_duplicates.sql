with baseline as (
    select id, ts, ec
    from {{ ref('clean_source_data') }}

    union all

    select id, ts, ec
    from {{ ref('interpolation_long_intervals') }}

    union all

    select id, ts, ec
    from {{ ref('interpolation_short_intervals') }}
)

select id
from baseline
where id = neighbor(id, 1)
  and ts = neighbor(ts, 1)