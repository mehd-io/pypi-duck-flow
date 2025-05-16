from ingestion.models import PypiJobParameters

PYPI_PUBLIC_DATASET = "bigquery-public-data.pypi.file_downloads"


def build_pypi_query(
    params: PypiJobParameters, pypi_public_dataset: str = PYPI_PUBLIC_DATASET
) -> str:
    """Build an optimized BigQuery query for PyPI file downloads
    # /!\ This is a large dataset, filter accordingly /!\
    """

    return f"""
    SELECT 
        timestamp,
        country_code,
        url,
        project,
        file,
        details,
        tls_protocol,
        tls_cipher
    FROM
        `{pypi_public_dataset}`
    WHERE
        project = ''{params.pypi_project}''
        AND {params.timestamp_column} >= TIMESTAMP("{params.start_date}")
        AND {params.timestamp_column} < TIMESTAMP("{params.end_date}")
    """
