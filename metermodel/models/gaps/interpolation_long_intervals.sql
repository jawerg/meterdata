{{config(order_by = ('id', 'ts'))}}

    
/* 
    Re-assemble the timeseries to fill each gap. In between the bounds, the standard
    load profile is used. The boundaries are taken as given.

 */
    
select
    bbd.id as id,
    slp.ts as ts,
    slp.ec * p.scaling_factor as ec
from {{ ref('boundaries_by_date') }} as bbd
inner join {{ ref('standard_load_profile') }} as slp on bbd.dt = slp.dt
inner join {{ ref('participants') }} as p on bbd.id = p.id
where slp.ts >= bbd.ts_start
  and slp.ts <= bbd.ts_end

union all

select id, ts, ec
from {{ ref('boundaries') }}

union all

select id, next_ts, next_ec
from {{ ref('boundaries') }}

order by id, ts
