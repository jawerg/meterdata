import time
from multiprocessing import Pool
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from clickhouse_driver import Client
import subprocess


def print_time_tracking_count_and_average():
    for func_name, times in globals()["time_tracking"].items():
        print(f"{func_name}: {len(times)} calls, {sum(times) / len(times)} avg. time")


def derive_parquet_data_from_csv_with_ddb_query(table_name: str):
    with duckdb.connect(database=":memory:") as conn:
        query_file_path = Path("queries") / "ddb" / f"{table_name}.sql"
        with open(query_file_path) as qry_file:
            conn.execute(qry_file.read())


def truncate_table(table: str):
    with Client("localhost") as client:
        client.execute(query=f"truncate table {table}")


def filter_table_by_year(table: pa.Table, year: int):
    year_array = pc.year(table.column("ts"))
    mask = pc.equal(year_array, pa.scalar(year))
    return table.filter(mask)


def get_data_from_row_group(file_path, year, row_group_index):
    parquet_file = pq.ParquetFile(
        file_path
    )  # Open the Parquet file within the worker process
    table = parquet_file.read_row_group(row_group_index)
    table = filter_table_by_year(table=table, year=year)
    data = list(zip(*[tuple(row) for row in table.to_pydict().values()]))
    return data


def insert_data(data):
    with Client("localhost") as client:
        client.execute(
            query="insert into meterdata_raw.meter_halfhourly_dataset (id, ts, val) values",
            params=data,
            types_check=True,
        )


def insert_row_group(args):
    file_path, year, row_group_index = args
    data = get_data_from_row_group(
        file_path=file_path,
        year=year,
        row_group_index=row_group_index,
    )
    insert_data(data=data)


def insert_file_content(table_name: str, year: int):
    file_path = f"data/ready/{table_name}.parquet"
    parquet_file = pq.ParquetFile(file_path)
    print(f"Number of row groups: {parquet_file.num_row_groups}")
    with Pool() as pool:
        pool.map(
            insert_row_group,
            [(file_path, year, i) for i in range(parquet_file.num_row_groups)],
        )


def get_row_count(table: str):
    with Client("localhost") as client:
        return client.execute(f"select count(*) from {table}")[0][0]


if __name__ == "__main__":
    truncate_table(table="meterdata.target")
    derive_parquet_data_from_csv_with_ddb_query(table_name=f"halfhourly_dataset")

    for y in [2011, 2012, 2013, 2014]:
        row_count = get_row_count(table="meterdata_raw.meter_halfhourly_dataset")
        truncate_table(table="meterdata_raw.meter_halfhourly_dataset")
        insert_file_content(table_name="halfhourly_dataset", year=y)

        start = time.time()
        subprocess.run("cd metermodel; dbt run", shell=True, text=True)
        duration = time.time() - start
        print(f"processed krows per second: {int((row_count / duration) / 1000)}")

        print_time_tracking_count_and_average()
