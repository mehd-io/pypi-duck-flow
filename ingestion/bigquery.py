import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from loguru import logger
import time
from ingestion.models import PypiJobParameters, FileDownloads
import pyarrow as pa

PYPI_PUBLIC_DATASET = "bigquery-public-data.pypi.file_downloads"


def build_pypi_query(
    params: PypiJobParameters, pypi_public_dataset: str = PYPI_PUBLIC_DATASET
) -> str:
    # Query the public PyPI dataset from BigQuery
    # /!\ This is a large dataset, filter accordingly /!\
    return f"""
    SELECT *
    FROM
        `{pypi_public_dataset}`
    WHERE
        project = '{params.pypi_project}'
        AND {params.timestamp_column} >= TIMESTAMP("{params.start_date}")
        AND {params.timestamp_column} < TIMESTAMP("{params.end_date}")
    """


def get_bigquery_client(project_name: str) -> bigquery.Client:
    """Get Big Query client"""
    try:
        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        if service_account_path:
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path
            )
            bigquery_client = bigquery.Client(
                project=project_name, credentials=credentials
            )
            return bigquery_client

        raise EnvironmentError(
            "No valid credentials found for BigQuery authentication."
        )

    except DefaultCredentialsError as creds_error:
        raise creds_error


2


def get_bigquery_result(
    query_str: str, bigquery_client: bigquery.Client, model: FileDownloads
) -> pa.Table:
    """Get query result from BigQuery and yield rows as dictionaries."""
    try:
        # Start measuring time
        start_time = time.time()
        # Run the query and directly load into a DataFrame
        logger.info(f"Running query: {query_str}")
        # dataframe = bigquery_client.query(query_str).to_dataframe(dtypes=FileDownloads().pandas_dtypes)
        pa_tbl = bigquery_client.query(query_str).to_arrow()
        # Log the time taken for query execution and data loading
        elapsed_time = time.time() - start_time
        logger.info(f"Query executed and data loaded in {elapsed_time:.2f} seconds")
        # Iterate over DataFrame rows and yield as dictionaries
        return pa_tbl

    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise
