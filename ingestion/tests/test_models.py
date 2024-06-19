import pandas as pd
import pytest
from pydantic import BaseModel
import duckdb
from ingestion.models import (
    validate_table,
    PypiJobParameters,
    TableValidationError,
    FileDownloads,
    File,
    Details,
    Installer,
    Implementation,
    Distro,
    System,
    Libc,
)
from ingestion.bigquery import build_pypi_query
import pyarrow as pa


class MyModel(BaseModel):
    column1: int
    column2: str
    column3: float


@pytest.fixture
def file_downloads_table():
    df = pd.DataFrame(
        {
            "timestamp": [
                pd.Timestamp("2023-01-01T12:00:00Z"),
                pd.Timestamp("2023-01-02T12:00:00Z"),
            ],
            "country_code": ["US", "CA"],
            "url": ["http://example.com/file1", "http://example.com/file2"],
            "project": ["project1", "project2"],
            "file": [
                File(
                    filename="file1.txt", project="project1", version="1.0", type="txt"
                ).dict(),
                File(
                    filename="file2.txt", project="project2", version="1.0", type="txt"
                ).dict(),
            ],
            "details": [
                Details(
                    installer=Installer(name="pip", version="21.0"),
                    python="3.8.5",
                    implementation=Implementation(name="CPython", version="3.8.5"),
                    distro=Distro(
                        name="Ubuntu",
                        version="20.04",
                        id="ubuntu2004",
                        libc=Libc(lib="glibc", version="2.31"),
                    ),
                    system=System(name="Linux", release="5.4.0-58-generic"),
                    cpu="x86_64",
                    openssl_version="1.1.1",
                    setuptools_version="50.3.0",
                    rustc_version="1.47.0",
                    ci=False,
                ).dict(),
                Details(
                    installer=Installer(name="pip", version="21.0"),
                    python="3.8.5",
                    implementation=Implementation(name="CPython", version="3.8.5"),
                    distro=Distro(
                        name="Ubuntu",
                        version="20.04",
                        id="ubuntu2004",
                        libc=Libc(lib="glibc", version="2.31"),
                    ),
                    system=System(name="Linux", release="5.4.0-58-generic"),
                    cpu="x86_64",
                    openssl_version="1.1.1",
                    setuptools_version="50.3.0",
                    rustc_version="1.47.0",
                    ci=False,
                ).dict(),
            ],
            "tls_protocol": ["TLSv1.2", "TLSv1.3"],
            "tls_cipher": ["AES128-GCM-SHA256", "AES256-GCM-SHA384"],
        }
    )

    return pa.Table.from_pandas(df)


def test_validate_table_with_valid_data():
    valid_data = {
        "column1": pa.array([1, 2]),
        "column2": pa.array(["a", "b"]),
        "column3": pa.array([1.1, 2.2]),
    }
    valid_table = pa.table(valid_data)
    errors = validate_table(valid_table, MyModel)
    assert not errors, f"Validation errors were found in valid data: {errors}"


def test_validate_table_with_valid_data():
    valid_data = {
        "column1": pa.array([1, 2]),
        "column2": pa.array(["a", "b"]),
        "column3": pa.array([1.1, 2.2]),
    }
    valid_table = pa.table(valid_data)
    errors = validate_table(valid_table, MyModel)
    assert not errors, f"Validation errors were found in valid data: {errors}"


def test_file_downloads_validation(file_downloads_table):
    try:
        validate_table(file_downloads_table, FileDownloads)
    except TableValidationError as e:
        pytest.fail(f"Table validation failed: {e}")


def test_build_pypi_query():
    params = PypiJobParameters(
        table_name="test_table",
        s3_path="s3://bucket/path",
        aws_profile="test_profile",
        gcp_project="test_project",
        start_date="2019-04-01",
        end_date="2023-11-30",
        timestamp_column="timestamp",
    )
    query = build_pypi_query(params)
    expected_query = f"""
    SELECT *
    FROM
        `bigquery-public-data.pypi.file_downloads`
    WHERE
        project = 'duckdb'
        AND timestamp >= TIMESTAMP("2019-04-01")
        AND timestamp < TIMESTAMP("2023-11-30")
    """
    assert query.strip() == expected_query.strip()


@pytest.fixture
def file_downloads_df():
    # Set up DuckDB in-memory database
    conn = duckdb.connect(database=":memory:", read_only=False)
    conn.execute(
        """
    CREATE TABLE tbl (
        timestamp TIMESTAMP WITH TIME ZONE, 
        country_code VARCHAR, 
        url VARCHAR, 
        project VARCHAR, 
        file STRUCT(filename VARCHAR, project VARCHAR, version VARCHAR, type VARCHAR), 
        details STRUCT(
            installer STRUCT(name VARCHAR, version VARCHAR), 
            python VARCHAR, 
            implementation STRUCT(name VARCHAR, version VARCHAR), 
            distro STRUCT(
                name VARCHAR, 
                version VARCHAR, 
                id VARCHAR, 
                libc STRUCT(lib VARCHAR, version VARCHAR)
            ), 
            system STRUCT(name VARCHAR, release VARCHAR), 
            cpu VARCHAR, 
            openssl_version VARCHAR, 
            setuptools_version VARCHAR, 
            rustc_version VARCHAR,
            ci BOOLEAN
        ), 
        tls_protocol VARCHAR, 
        tls_cipher VARCHAR
    )
    """
    )

    # Load data from CSV
    conn.execute("COPY tbl FROM 'ingestion/tests/sample_file_downloads.csv' (HEADER)")
    # Create DataFrame
    return conn.execute("SELECT * FROM tbl").df()
