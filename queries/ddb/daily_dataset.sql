copy (
    select *
    from read_csv(
            'data/raw/daily_dataset/daily_dataset/block_*.csv',
            delim = ',',
            header = true,
            columns ={
                'LCLid': 'VARCHAR',
            'day': 'DATE',
            'energy_median': 'FLOAT',
            'energy_mean': 'FLOAT',
            'energy_max': 'FLOAT',
            'energy_count': 'FLOAT',
            'energy_std': 'FLOAT',
            'energy_sum': 'FLOAT',
            'energy_min': 'FLOAT' }
        )
    )
    to 'data/ready/daily_dataset.parquet' (format 'parquet', compression 'zstd');
