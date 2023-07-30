{{
    config(
        partition_by='halfMD5(id) % 64',
        order_by=('id', 'ts')
    )
}}

with
data_union as (
    select id, ts, ec
    from {{ ref('clean_and_partitioned_source_data') }}

    union all

    select id, ts, ec
    from {{ ref('interpolation_long_intervals') }}

    union all

    select id, ts, ec
    from {{ ref('interpolation_short_intervals') }}

    union all

    select id, ts, ec
    from {{ ref('interpolation_clean_data') }}
)

select id, ts, ec
from data_union
order by id, ts
