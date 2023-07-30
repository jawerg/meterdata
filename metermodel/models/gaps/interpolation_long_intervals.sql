{{config(order_by=('id', 'gap_id', 'ts'))}}

    
/* 
    Re-assemble the timeseries to fill each gap. In between the bounds, the standard
    load profile is used. The boundaries are taken as given.

 */
    
select
    bbd.id as id,
    bbd.gap_id as gap_id,
    slp.ts as ts,
    slp.ec * p.scaling_factor as ec
from {{ ref('boundaries_by_date') }} as bbd
inner join {{ ref('standard_load_profile') }} as slp on bbd.dt = slp.dt
inner join {{ ref('participants') }} as p on bbd.id = p.id
where slp.ts >= bbd.start_ts
  and slp.ts <= bbd.end_ts
  and (id, ts) not in (
    select id, ts
    from meterdata_gaps.interpolation_long_interval_bounds
)
order by id, gap_id, ts
