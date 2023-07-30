{{
    config(
        partition_by='halfMD5(id) % 64',
        order_by=('id', 'ts')
    )
}}

/*
    As most timeseries do not contain gaps, or at least not of their full length, this
    is meant to reduce the size of data to be interpolated by exactly finding the
    boundaries of each "gap". This means, a single id can have multiple gaps.
    The idea is to carry the information of the bounds (current and next) through.

 */

select
    id,
    rowNumberInAllBlocks() as gap_id,
    ts,
    neighbor(ts, 1) as next_ts,
    ec,
    neighbor(ec, 1) as next_ec
from {{ ref('clean_and_partitioned_source_data') }}
where next_ts - ts > 30 * 60 -- fill gaps larger than 30 mins
  and neighbor(id, 1) = id
order by id, ts
settings optimize_read_in_order = 0
