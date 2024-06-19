from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_pypi_query,
)
import duckdb
from datetime import datetime
from loguru import logger
from ingestion.duck import (
    create_table_from_dataframe,
    load_aws_credentials,
    write_to_s3_from_duckdb,
    write_to_md_from_duckdb,
    connect_to_md,
)
import fire
from ingestion.models import (
    validate_table,
    FileDownloads,
    PypiJobParameters,
    duckdb_ddl_file_downloads,
)
import os


def main(params: PypiJobParameters):
    start_time = datetime.now()
    # Loading data from BigQuery
    pa_tbl = get_bigquery_result(
        query_str=build_pypi_query(params),
        bigquery_client=get_bigquery_client(project_name=params.gcp_project),
        model=FileDownloads,
    )
    validate_table(pa_tbl, FileDownloads)
    # Loading to DuckDB
    conn = duckdb.connect()
    create_table_from_dataframe(
        duckdb_con=conn,
        table_name=params.table_name,
        table_ddl=duckdb_ddl_file_downloads("pypi_file_downloads"),
    )

    logger.info(f"Sinking data to {params.destination}")
    if "local" in params.destination:
        conn.sql(f"COPY {params.table_name} TO '{params.table_name}.csv';")

    if "s3" in params.destination:
        load_aws_credentials(conn, params.aws_profile)
        write_to_s3_from_duckdb(
            conn, f"{params.table_name}", params.s3_path, "timestamp"
        )

    if "md" in params.destination:
        connect_to_md(conn, os.environ["motherduck_token"])
        write_to_md_from_duckdb(
            duckdb_con=conn,
            table=f"{params.table_name}",
            local_database="memory",
            remote_database="pypi",
            timestamp_column=params.timestamp_column,
            start_date=params.start_date,
            end_date=params.end_date,
        )
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    logger.info(
        f"Total job completed in {elapsed // 60} minutes and {elapsed % 60:.2f} seconds."
    )


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))
