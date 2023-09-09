import subprocess
import time

from meterdata.loader.clickhouse import (
    ch_truncate_table,
    ch_insert_file_content,
    CH_BASE_TABLE,
)
from meterdata.loader.common import (
    derive_parquet_data_from_csv_with_ddb_query,
    create_empty_exec_time_file,
    write_execution_time,
)
from meterdata.loader.postgres import pg_truncate_tables, pg_insert_file_content

if __name__ == "__main__":
    years = [2011, 2012, 2013, 2014]

    create_empty_exec_time_file()
    derive_parquet_data_from_csv_with_ddb_query(
        table_name=f"halfhourly_dataset",
        years=years,
    )

    # clean existing tables first
    ch_truncate_table(table=CH_BASE_TABLE)
    pg_truncate_tables()

    # now run batch by batch per year.
    for y in years:
        ch_insert_file_content(table_name="halfhourly_dataset", year=y)
        pg_insert_file_content(table_name="halfhourly_dataset", year=y)

    # feel free to move into loop, but now ch vs pg is at the center of the comparison
    start = time.perf_counter()
    subprocess.run("cd metermodel; dbt run", shell=True, text=True)
    write_execution_time(
        exec_time=time.perf_counter() - start,
        function_name="dbt_run",
    )
