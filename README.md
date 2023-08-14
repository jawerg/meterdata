# Meterdata

I've found some nice data on kaggle, which should be similar to what we use:
[Smart Meters in London](https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london).

The idea is to convert csv files to parquet files and then load them into clickhouse.
At the heart of this is the throughput, so how many MB can be processed per second.
