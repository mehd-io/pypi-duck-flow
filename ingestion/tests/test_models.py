import pytest
from ingestion.models import PypiJobParameters


def test_pypi_job_parameters_defaults():
    params = PypiJobParameters(gcp_project="test_project")
    assert params.start_date == "2019-04-01"
    assert params.end_date == "2023-11-30"
    assert params.pypi_project == "duckdb"
    assert params.database_name == "duckdb_stats"
    assert params.table_name == "pypi_file_downloads"
    assert params.timestamp_column == "timestamp"
    assert params.destination == ["local"]


def test_pypi_job_parameters_custom_values():
    params = PypiJobParameters(
        gcp_project="test_project",
        start_date="2020-01-01",
        end_date="2024-01-01",
        pypi_project="test_project",
        database_name="test_db",
        table_name="test_table",
        timestamp_column="created_at",
        destination="s3"
    )
    assert params.start_date == "2020-01-01"
    assert params.end_date == "2024-01-01"
    assert params.pypi_project == "test_project"
    assert params.database_name == "test_db"
    assert params.table_name == "test_table"
    assert params.timestamp_column == "created_at"
    assert params.destination == "s3"


def test_pypi_job_parameters_destination_types():
    # Test with string destination
    params_str = PypiJobParameters(gcp_project="test_project", destination="s3")
    assert params_str.destination == "s3"

    # Test with list destination
    params_list = PypiJobParameters(gcp_project="test_project", destination=["s3", "md"])
    assert params_list.destination == ["s3", "md"]

    # Test with default destination
    params_default = PypiJobParameters(gcp_project="test_project")
    assert params_default.destination == ["local"]
