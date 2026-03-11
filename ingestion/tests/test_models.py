import pytest
from ingestion.models import PypiJobParameters
from ingestion.bigquery import build_bigquery_filter, PYPI_PUBLIC_TABLE, COLUMNS


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
        destination="s3",
    )
    assert params.start_date == "2020-01-01"
    assert params.end_date == "2024-01-01"
    assert params.pypi_project == "test_project"
    assert params.database_name == "test_db"
    assert params.table_name == "test_table"
    assert params.timestamp_column == "created_at"
    assert params.destination == "s3"


def test_pypi_job_parameters_destination_types():
    params_str = PypiJobParameters(gcp_project="test_project", destination="s3")
    assert params_str.destination == "s3"

    params_list = PypiJobParameters(
        gcp_project="test_project", destination=["s3", "md"]
    )
    assert params_list.destination == ["s3", "md"]

    params_default = PypiJobParameters(gcp_project="test_project")
    assert params_default.destination == ["local"]


def test_build_bigquery_filter():
    params = PypiJobParameters(
        gcp_project="test_project",
        start_date="2023-01-01",
        end_date="2023-02-01",
        pypi_project="duckdb",
    )
    filter_str = build_bigquery_filter(params)
    assert 'project = "duckdb"' in filter_str
    assert 'TIMESTAMP("2023-01-01")' in filter_str
    assert 'TIMESTAMP("2023-02-01")' in filter_str
    assert "timestamp >=" in filter_str
    assert "timestamp <" in filter_str


def test_build_bigquery_filter_custom_timestamp():
    params = PypiJobParameters(
        gcp_project="test_project",
        timestamp_column="created_at",
        start_date="2024-06-01",
        end_date="2024-07-01",
    )
    filter_str = build_bigquery_filter(params)
    assert "created_at >=" in filter_str
    assert "created_at <" in filter_str


def test_columns_list():
    assert len(COLUMNS) == 8
    assert "timestamp" in COLUMNS
    assert "project" in COLUMNS
    assert "country_code" in COLUMNS
    assert "file" in COLUMNS
    assert "details" in COLUMNS


def test_pypi_public_table():
    assert PYPI_PUBLIC_TABLE == "bigquery-public-data.pypi.file_downloads"
