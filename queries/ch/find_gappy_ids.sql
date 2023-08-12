select
    id,
    count(*) as n_obs,
    sum(case when neighbor(ts, 1) - ts = 60*60 then 1 else 0 end) as n_gaps
from meterdata.target
where neighbor(id, 1) = id
  and neighbor(ts, 1) is not null
group by id
order by n_gaps desc
limit 10;
