from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_pypi_query,
)
from datetime import datetime
from loguru import logger
from ingestion.duck import ArrowTableLoadingBuffer
import fire
from ingestion.models import (
    validate_table,
    FileDownloads,
    PypiJobParameters,
)


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
    logger.info(f"Sinking data to {params.destination}")
    # Determine the initial destination (local or md)
    initial_destination = "local" if params.destination == "s3" else params.destination
    logger.info(f"Initial data sink: {initial_destination}")

    buffer = ArrowTableLoadingBuffer(
        duckdb_schema=FileDownloads.duckdb_schema(params.table_name),
        pyarrow_schema=FileDownloads.pyarrow_schema(),
        database_name=params.database_name,
        table_name=params.table_name,
        dryrun=False,
        destination=initial_destination,
    )
    logger.info(f"Deleting existing data from {params.start_date} to {params.end_date}")
    delete_query = f"""
    DELETE FROM {params.database_name}.main.{params.table_name} 
    WHERE {params.timestamp_column} >= '{params.start_date}' AND {params.timestamp_column} < '{params.end_date}'
    """
    buffer.conn.execute(delete_query)
    logger.info("Existing data deleted")

    buffer.insert(pa_tbl)

    # If destination is S3, write to S3
    if params.destination == "s3":
        logger.info("Writing data to S3")
        buffer.write_to_s3(
            s3_path=params.s3_path,
            timestamp_column=params.timestamp_column,
            aws_profile=params.aws_profile,
        )
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    logger.info(
        f"Total job completed in {elapsed // 60} minutes and {elapsed % 60:.2f} seconds."
    )


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))
