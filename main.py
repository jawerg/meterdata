import shutil
import textwrap
import time
from multiprocessing import Pool
from pathlib import Path

import clickhouse_connect
import duckdb
import psycopg2
import pyarrow.parquet as pq
from clickhouse_driver import Client
from psycopg2.extras import execute_values


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
def truncate_table(table_name: str):
    with Client("localhost") as client:
        client.execute(query=f"truncate table meterdata_raw.meter_{table_name}")


def get_data_from_row_group(file_path, row_group_index):
    parquet_file = pq.ParquetFile(file_path)  # Open the Parquet file within the worker process
    table = parquet_file.read_row_group(row_group_index)
    table = table.rename_columns(["id", "ts", "val"])
    data = list(zip(*[tuple(row) for row in table.to_pydict().values()]))
    return data


@measure_execution_time
def insert_data(data):
    with Client("localhost") as client:
        client.execute(
            query="insert into meterdata_raw.meter_halfhourly_dataset (id, ts, val) values",
            params=data,
            types_check=True,
        )


def insert_row_group(args):
    file_path, row_group_index = args
    data = get_data_from_row_group(file_path=file_path, row_group_index=row_group_index)
    insert_data(data=data)


@measure_execution_time
def insert_file_content(table_name: str):
    file_path = f"data/ready/{table_name}.parquet"
    parquet_file = pq.ParquetFile(file_path)
    print(f"Number of row groups: {parquet_file.num_row_groups}")
    with Pool() as pool:
        pool.map(insert_row_group, [(file_path, i) for i in range(parquet_file.num_row_groups)])


def insert_data_into_postgres(data):
    dbname, user, password, host, port = "meterdata", "postgres", "postgres", "localhost", "5432"
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    qry = "insert into raw.meter_halfhourly_dataset_base(id, ts, val) values %s"
    execute_values(cur, qry, data)
    conn.commit()
    cur.close()
    conn.close()


def insert_row_group_into_postgres(args):
    file_path, row_group_index = args
    data = get_data_from_row_group(file_path=file_path, row_group_index=row_group_index)
    insert_data_into_postgres(data=data)


@measure_execution_time
def insert_file_content_into_postgres(table_name: str):
    file_path = f"data/ready/{table_name}.parquet"
    parquet_file = pq.ParquetFile(file_path)
    print(f"Number of row groups: {parquet_file.num_row_groups}")
    with Pool() as pool:
        pool.map(
            insert_row_group_into_postgres,
            [(file_path, i) for i in range(parquet_file.num_row_groups)],
        )


@measure_execution_time
def core(table_name: str):
    derive_parquet_data_from_csv_with_ddb_query(table_name=table_name)
    move_resulting_file_to_clickhouse_files_folder(table_name=table_name)
    load_file_into_according_clickhouse_table(table_name=table_name)


if __name__ == "__main__":
    core(table_name="halfhourly_dataset")
    truncate_table(table_name="halfhourly_dataset")
    insert_file_content(table_name="halfhourly_dataset")
    # insert_file_content_into_postgres(table_name="halfhourly_dataset")
