from ingestion.bigquery import build_bigquery_filter, PYPI_PUBLIC_TABLE, COLUMNS
from datetime import datetime
from loguru import logger
from ingestion.duck import MotherDuckBigQueryLoader
import fire
from ingestion.models import PypiJobParameters


def main(params: PypiJobParameters):
    start_time = datetime.now()

    loader = MotherDuckBigQueryLoader(
        motherduck_database=params.database_name,
        motherduck_table=params.table_name,
        project_id=params.gcp_project,
    )

    bq_filter = build_bigquery_filter(params)

    loader.load_from_bigquery_to_motherduck(
        table=PYPI_PUBLIC_TABLE,
        filter_str=bq_filter,
        columns=COLUMNS,
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
