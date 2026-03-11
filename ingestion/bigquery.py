from ingestion.models import PypiJobParameters

PYPI_PUBLIC_TABLE = "bigquery-public-data.pypi.file_downloads"

COLUMNS = [
    "timestamp",
    "country_code",
    "url",
    "project",
    "file",
    "details",
    "tls_protocol",
    "tls_cipher",
]


def build_bigquery_filter(params: PypiJobParameters) -> str:
    """Build a BigQuery Storage Read API row restriction filter for PyPI file downloads.
    This filter is passed directly to bigquery_scan's filter parameter.
    """
    return (
        f'project = "{params.pypi_project}" '
        f'AND {params.timestamp_column} >= TIMESTAMP("{params.start_date}") '
        f'AND {params.timestamp_column} < TIMESTAMP("{params.end_date}")'
    )
