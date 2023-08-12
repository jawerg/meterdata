import shutil
import textwrap
import time
from multiprocessing import Pool
from pathlib import Path

import clickhouse_connect
import duckdb
import pyarrow.parquet as pq
from clickhouse_driver import Client


def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{kwargs.get('table_name')}: {func.__name__}: {execution_time:.4f} seconds.")
        return result

    return wrapper


@measure_execution_time
def derive_parquet_data_from_csv_with_ddb_query(table_name: str):
    with duckdb.connect(database=":memory:") as conn:
        query_file_path = Path("queries") / "ddb" / f"{table_name}.sql"
        with open(query_file_path) as qry_file:
            conn.execute(qry_file.read())


@measure_execution_time
def move_resulting_file_to_clickhouse_files_folder(table_name: str):
    # Move the resulting parquet file to the clickhouse folder
    shutil.copy(
        src=f"data/ready/{table_name}.parquet",
        dst=f"/opt/homebrew/var/lib/clickhouse/user_files/{table_name}.parquet",
    )


@measure_execution_time
def load_file_into_according_clickhouse_table(table_name: str):
    with clickhouse_connect.get_client(host="localhost") as client:
        client.command(
            textwrap.dedent(
                f"""
                insert into meterdata_raw.meter_{table_name} 
                select * from file('{table_name}.parquet', Parquet)
                """.strip()
            )
        )


@measure_execution_time
def insert_data(data):
    with Client("localhost") as client:
        client.execute(
            query=textwrap.dedent(
                """
                insert into meterdata_raw.meter_halfhourly_dataset (
                    `LCLid`,
                    `tstp`,
                    `energy(kWh/hh)`
                ) 
                values
                """
            ).strip(),
            params=data,
            types_check=True,
        )


def process_and_insert_row_group(args):
    file_path, row_group_index = args
    parquet_file = pq.ParquetFile(file_path)  # Open the Parquet file within the worker process
    table = parquet_file.read_row_group(row_group_index)
    table = table.rename_columns(["id", "ts", "val"])
    data = list(zip(*[tuple(row) for row in table.to_pydict().values()]))
    insert_data(data=data)


@measure_execution_time
def insert_file_content(table_name: str):
    with Client("localhost") as client:
        client.execute(query=f"truncate table meterdata_raw.meter_{table_name}")

    file_path = f"data/ready/{table_name}.parquet"
    parquet_file = pq.ParquetFile(file_path)
    print(f"Number of row groups: {parquet_file.num_row_groups}")

    # Create a pool of worker processes
    with Pool() as pool:
        # Use the pool to apply the process_and_insert_row_group function to each row group
        pool.map(
            process_and_insert_row_group,
            [(file_path, i) for i in range(parquet_file.num_row_groups)],
        )


@measure_execution_time
def core(table_name: str):
    derive_parquet_data_from_csv_with_ddb_query(table_name=table_name)
    move_resulting_file_to_clickhouse_files_folder(table_name=table_name)
    load_file_into_according_clickhouse_table(table_name=table_name)


if __name__ == "__main__":
    core(table_name="halfhourly_dataset")
    # insert_file_content(table_name="halfhourly_dataset")
