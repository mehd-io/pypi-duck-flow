from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_pypi_query,
)
import duckdb
from loguru import logger
from ingestion.duck import (
    create_table_from_dataframe,
    load_aws_credentials,
    write_to_s3_from_duckdb,
    write_to_md_from_duckdb,
    connect_to_md,
)
import fire
from ingestion.models import validate_dataframe, FileDownloads, PypiJobParameters
import os


def main(params: PypiJobParameters):
    # Loading data from BigQuery
    df = get_bigquery_result(
        query_str=build_pypi_query(params),
        bigquery_client=get_bigquery_client(project_name=params.gcp_project),
    )
    validate_dataframe(df, FileDownloads)
    # Loading to DuckDB
    conn = duckdb.connect()
    create_table_from_dataframe(conn, params.table_name, "df")

    logger.info(f"Sinking data to {params.destination}")
    if "local" in params.destination:
        conn.sql(f"COPY {params.table_name} TO '{params.table_name}.csv';")

    if "s3" in params.destination:
        # install_extensions(conn, params.extensions)
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


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))
