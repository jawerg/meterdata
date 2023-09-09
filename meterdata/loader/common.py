import time
from pathlib import Path

import duckdb
import pyarrow as pa
from pyarrow import parquet as pq


def create_empty_exec_time_file():
    with open("exec_time.csv", "w") as f:
        f.write("function,exec_time,n_rows,year,row_group\n")


def write_execution_time(
    exec_time: float,
    function_name: str,
    n_rows: int | None = None,
    year: int | None = None,
    row_group: int | None = None,
):
    """looks like decorators are not working easy with multiprocessing"""
    with open("exec_time.csv", "a") as f:
        if n_rows:
            f.write(f"{function_name},{exec_time},{n_rows},{year},{row_group}\n")
        f.write(f"{function_name},{exec_time}\n")


def derive_parquet_data_from_csv_with_ddb_query(table_name: str, years: list[int]):
    start = time.perf_counter()

    with duckdb.connect(database=":memory:") as conn:
        query_file_path = Path("queries") / "ddb" / f"{table_name}.sql"
        for year in years:
            with open(query_file_path) as qry_file:
                conn.execute(qry_file.read().replace("{{ year }}", str(year)))

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=derive_parquet_data_from_csv_with_ddb_query.__name__,
    )


def get_row_group(file_path, year, row_group):
    start = time.perf_counter()

    parquet_file = pq.ParquetFile(file_path)  # Open file within the worker process!
    table = parquet_file.read_row_group(row_group)

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=get_row_group.__name__,
    )
    return table


def pyarrow_table_to_pylist(table: pa.Table):
    start = time.perf_counter()
    data = list(zip(*[tuple(row) for row in table.to_pydict().values()]))

    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name=pyarrow_table_to_pylist.__name__,
    )
    return data
