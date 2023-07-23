copy (
    select *
    from read_csv(
            'data/raw/halfhourly_dataset/halfhourly_dataset/block_*.csv',
            delim = ',',
            header = true,
            nullstr = 'Null',
            columns ={
                'LCLid': 'VARCHAR',
            'tstp': 'TIMESTAMP',
            'energy(kWh/hh)': 'FLOAT' }
        )
    )
    to 'data/ready/halfhourly_dataset.parquet' (format 'parquet', compression 'zstd');
