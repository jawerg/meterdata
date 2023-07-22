{{config(order_by = ('id', 'ts'))}}


select
    gaps.id as id,
    slp.ts as ts,
    slp.ec as ec --* p.scaling_factor as ec
from {{ ref('timeseries_gaps') }} as gaps
inner join {{ ref('standard_load_profile') }} as slp on gaps.dt = slp.dt
inner join {{ ref('participants') }} as p on gaps.id = p.id
where slp.ts >= gaps.ts_start
  and slp.ts <= gaps.ts_end

union all

select id, ts, ec
from {{ ref('timeseries_gap_bounds') }}

union all

select id, next_ts, next_ec
from {{ ref('timeseries_gap_bounds') }}

order by id, ts
