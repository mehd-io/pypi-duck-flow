from pydantic import BaseModel
from typing import Union, List


class PypiJobParameters(BaseModel):
    """Parameters for PyPI data ingestion job"""

    start_date: str = "2019-04-01"
    end_date: str = "2023-11-30"
    pypi_project: str = "duckdb"
    database_name: str = "duckdb_stats"
    table_name: str = "pypi_file_downloads"
    gcp_project: str
    timestamp_column: str = "timestamp"
    destination: Union[List[str], str] = ["local"]  # local, s3, md
