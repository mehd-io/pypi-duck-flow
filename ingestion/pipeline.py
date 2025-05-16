from ingestion.bigquery import build_pypi_query
from datetime import datetime
from loguru import logger
from ingestion.duck import MotherDuckBigQueryLoader
import fire
from ingestion.models import PypiJobParameters


def main(params: PypiJobParameters):
    start_time = datetime.now()

    # Initialize the MotherDuck loader
    loader = MotherDuckBigQueryLoader(
        motherduck_database=params.database_name,
        motherduck_table=params.table_name,
        project_id=params.gcp_project,
    )

    # Build BigQuery query
    query = build_pypi_query(params)

    # Load data from BigQuery to MotherDuck
    loader.load_from_bigquery_to_motherduck(
        query=query,
        timestamp_column=params.timestamp_column,
        start_date=params.start_date,
        end_date=params.end_date,
    )

    # Log total execution time
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    logger.info(
        f"Total job completed in {elapsed // 60} minutes and {elapsed % 60:.2f} seconds."
    )


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))
