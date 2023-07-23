# Meterdata

I've found some nice data on kaggle, which should be similar to what we use:
[Smart Meters in London](https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london).

The idea is to convert csv files to parquet files and then load them into clickhouse.
At the heart of this is the throughput, so how many MB can be processed per second.

Theoretically, the parquet file could even be skipped, as CSV can be loaded too, however
I'm worried about possible network or disk IO bottlenecks in mounted Kubenetes volumes.
Let's see. It can only get better^^ In general though, I like the idea of making the
files smaller, as it will make the whole process faster.

So, let's reflect on the numbers here:

1. The small dataset consists of 115 different files that sum up to 375 MB.
    - Converting those files into a single parquet file takes 0.4 seconds, which means
      that the throughput is 937.5 MB/s. The resulting parquet file is 53 MB in size,
      thus, the compression factor is around 7.
    - Moving the file to the other folder should be the critical part, when concerning
      mounted drives. I've got no clue how this works in Kubernetes...
    - Loading the file into clickhouse takes 0.66 seconds, which means that the
      throughput is 80 MB/s.
2. The large dataset consists of 115 (too) different files that sum up to 7500 MB
   - Converting those files into a single parquet file takes 7 seconds, which means
     that the throughput is 1071 MB/s. The resulting parquet file is 457 MB in size,
     thus, the compression factor is around 16.
   - ...
   - Loading the file into clickhouse takes 12.9 seconds, which means that the
     throughput is 35 MB/s.

So, applying the logic from step 2 to 1 TB of data, would take at least 133 times as 
long, which is 44 minutes (133 * (7s + 13s) / 60). If loading a TB of raw data into
Clickhouse would take less than an hour, that would be amazing. However, note that 
the Macbooks have very good disks and no network was involved when moving files around.

So, one question remains: What is the size of those tables in clickhouse? The larger 
table takes 1114 MB of disk space, which is a compression factor of 6.7 compared to the
CSV file sizes.

To check if loading times worsen with more data, I loaded the same data 10 times. 
Probably not the best testing strategy. But it seems that the loading times are constant
over those attempts around 20s for the 7.5 GB of CSV files.

Notes:
- Timestamps are still off...

## Logs

```
daily_dataset: derive_parquet_data_from_csv_with_ddb_query: 0.4276 seconds.
daily_dataset: move_resulting_file_to_clickhouse_files_folder: 0.0016 seconds.
daily_dataset: load_file_into_according_clickhouse_table: 0.6699 seconds.

halfhourly_dataset: derive_par  quet_data_from_csv_with_ddb_query: 6.9556 seconds.
halfhourly_dataset: move_resulting_file_to_clickhouse_files_folder: 0.0002 seconds.
halfhourly_dataset: load_file_into_according_clickhouse_table: 12.8965 seconds.
```

## Dataset size

```
7.9G	data/raw/halfhourly_dataset/halfhourly_dataset
7.9G	data/raw/halfhourly_dataset
1.7G	data/raw/hhblock_dataset/hhblock_dataset
1.7G	data/raw/hhblock_dataset
375M	data/raw/daily_dataset/daily_dataset
375M	data/raw/daily_dataset
 10G	data/raw
```
