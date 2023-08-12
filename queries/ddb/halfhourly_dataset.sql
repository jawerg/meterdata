copy (
    select
        "LCLid" as id,
        "tstp" as ts,
        "energy(kWh/hh)" as val
    from read_csv(
            'data/raw/halfhourly_dataset/halfhourly_dataset/block_*.csv',
            delim = ',',
            header = true,
            nullstr = 'Null',
            columns ={ 'LCLid': 'VARCHAR', 'tstp': 'TIMESTAMP', 'energy(kWh/hh)': 'FLOAT' }
        )
    where "energy(kWh/hh)" is not null
    )
    to 'data/ready/halfhourly_dataset.parquet' (
        format 'parquet',
        compression 'zstd',
        row_group_size 2000000
);
