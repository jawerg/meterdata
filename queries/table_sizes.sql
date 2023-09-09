select parts.*,
       columns.compressed_size,
       columns.uncompressed_size,
       columns.compression_ratio,
       columns.compression_percentage
from (
         select table,
                formatReadableSize(sum(data_uncompressed_bytes))          as uncompressed_size,
                formatReadableSize(sum(data_compressed_bytes))            as compressed_size,
                round(sum(data_compressed_bytes) / sum(data_uncompressed_bytes), 3) as  compression_ratio,
                round((100 - (sum(data_compressed_bytes) * 100) / sum(data_uncompressed_bytes)), 3) as compression_percentage

         from system.columns
         group by table )      columns
     right join (
    select table,
           sum(rows)                                            as rows,
           max(modification_time)                               as latest_modification,
           formatReadableSize(sum(bytes))                       as disk_size,
           formatReadableSize(sum(primary_key_bytes_in_memory)) as primary_keys_size,
           any(engine)                                          as engine,
           sum(bytes)                                           as bytes_size
    from system.parts
    where active
    group by database, table ) parts
    on columns.table = parts.table
where table like '%meter%' or table like '%timeseries%'
order by parts.bytes_size desc;
