import shutil
import textwrap
from pathlib import Path

import clickhouse_connect
import duckdb

import time


def measure_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{kwargs.get('query_name')}: {func.__name__}: {execution_time:.4f} seconds.")
        return result

    return wrapper


@measure_execution_time
def derive_parquet_data_from_csv_with_ddb_query(query_name: str):
    with duckdb.connect(database=":memory:") as conn:
        query_file_path = Path("queries") / "ddb" / f"{query_name}.sql"
        with open(query_file_path) as qry_file:
            conn.execute(qry_file.read())


@measure_execution_time
def move_resulting_file_to_clickhouse_files_folder(query_name: str):
    # Move the resulting parquet file to the clickhouse folder
    shutil.move(
        src=f"data/ready/{query_name}.parquet",
        dst=f"/opt/homebrew/var/lib/clickhouse/user_files/{query_name}.parquet",
    )


@measure_execution_time
def load_file_into_according_clickhouse_table(query_name: str):
    with clickhouse_connect.get_client(host="localhost") as client:
        client.command(
            textwrap.dedent(
                f"""
                insert into meterdata_raw.meter_{query} 
                select * from file('{query}.parquet', Parquet)
                """.strip()
            )
        )


@measure_execution_time
def core(query_name: str):
    derive_parquet_data_from_csv_with_ddb_query(query_name=query_name)
    move_resulting_file_to_clickhouse_files_folder(query_name=query_name)
    load_file_into_according_clickhouse_table(query_name=query_name)


if __name__ == "__main__":
    csv2pqt_queries = [
        "daily_dataset",
        "halfhourly_dataset",
    ]
    for query in csv2pqt_queries:
        core(query_name=query)
