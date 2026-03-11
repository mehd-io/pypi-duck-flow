import pytest
from unittest.mock import patch, MagicMock, ANY
from ingestion.duck import MotherDuckBigQueryLoader, DataQualityError


@pytest.fixture
def mock_duckdb():
    with patch("duckdb.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        yield mock_conn


@pytest.fixture
def loader(mock_duckdb):
    with patch.dict("os.environ", {"motherduck_token": "test_token", "HOME": "/tmp"}):
        loader = MotherDuckBigQueryLoader(
            motherduck_database="test_db",
            motherduck_table="test_table",
            project_id="test_project",
        )
        loader.conn = mock_duckdb
        return loader


def test_loader_initialization(loader, mock_duckdb):
    assert loader.motherduck_database == "test_db"
    assert loader.motherduck_table == "test_table"
    assert loader.project_id == "test_project"

    mock_duckdb.execute.assert_any_call("INSTALL bigquery FROM community;")
    mock_duckdb.execute.assert_any_call("LOAD bigquery;")
    mock_duckdb.execute.assert_any_call("SET bq_experimental_filter_pushdown = true")
    mock_duckdb.execute.assert_any_call("SET preserve_insertion_order = FALSE")
    mock_duckdb.execute.assert_any_call("ATTACH 'md:'")
    mock_duckdb.execute.assert_any_call("CREATE DATABASE IF NOT EXISTS test_db")


def test_loader_initialization_without_token():
    with patch.dict("os.environ", {"HOME": "/tmp"}, clear=True):
        with pytest.raises(ValueError, match="MotherDuck token is required"):
            MotherDuckBigQueryLoader(
                motherduck_database="test_db",
                motherduck_table="test_table",
                project_id="test_project",
            )


def test_load_from_bigquery(loader, mock_duckdb):
    mock_duckdb.execute.return_value.fetchone.return_value = (500,)

    result = loader._load_from_bigquery(
        table="bigquery-public-data.pypi.file_downloads",
        filter_str='project = "duckdb" AND timestamp >= TIMESTAMP("2023-01-01")',
        columns=["timestamp", "project", "country_code"],
    )

    assert result == 500

    sql_calls = [str(call[0][0]) for call in mock_duckdb.execute.call_args_list]
    bigquery_scan_calls = [c for c in sql_calls if "bigquery_scan" in c]
    assert len(bigquery_scan_calls) > 0
    assert any("billing_project='test_project'" in c for c in bigquery_scan_calls)
    assert any("filter=" in c for c in bigquery_scan_calls)
    assert any("timestamp, project, country_code" in c for c in bigquery_scan_calls)


def test_load_from_bigquery_to_motherduck(loader, mock_duckdb):
    mock_duckdb.execute.return_value.fetchone.side_effect = [
        (1000,),  # _load_from_bigquery: row count
        (1000, 1000, 1000, "2023-01-01", "2023-01-31"),  # _validate_data
        (True,),  # _delete_existing_data: table exists check
        (1000,),  # _copy_to_motherduck: total rows
    ] + [(None,)] * 20  # chunked inserts (extra for safety)

    result = loader.load_from_bigquery_to_motherduck(
        table="bigquery-public-data.pypi.file_downloads",
        filter_str='project = "duckdb"',
        columns=["timestamp", "project"],
        timestamp_column="timestamp",
        start_date="2023-01-01",
        end_date="2023-01-31",
        chunk_size=100,
    )

    assert result == 1000
    sql_calls = [str(call[0][0]) for call in mock_duckdb.execute.call_args_list]
    assert any("bigquery_scan" in c for c in sql_calls)


def test_delete_existing_data(loader, mock_duckdb):
    mock_duckdb.execute.return_value.fetchone.return_value = (True,)

    loader._delete_existing_data(
        timestamp_column="timestamp",
        start_date="2023-01-01",
        end_date="2023-01-31",
    )

    sql_calls = [str(call[0][0]) for call in mock_duckdb.execute.call_args_list]
    delete_calls = [c for c in sql_calls if "DELETE FROM" in c]
    assert len(delete_calls) > 0
    assert any("test_db.main.test_table" in c for c in delete_calls)
    assert any("timestamp >= '2023-01-01'" in c for c in delete_calls)
    assert any("timestamp < '2023-01-31'" in c for c in delete_calls)


def test_delete_skips_when_table_missing(loader, mock_duckdb):
    mock_duckdb.execute.return_value.fetchone.return_value = (False,)

    loader._delete_existing_data(
        timestamp_column="timestamp",
        start_date="2023-01-01",
        end_date="2023-01-31",
    )

    sql_calls = [str(call[0][0]) for call in mock_duckdb.execute.call_args_list]
    delete_calls = [c for c in sql_calls if "DELETE FROM" in c]
    assert len(delete_calls) == 0


def test_copy_to_motherduck(loader, mock_duckdb):
    mock_duckdb.execute.return_value.fetchone.return_value = (1000,)

    result = loader._copy_to_motherduck(chunk_size=100)

    assert result == 1000

    sql_calls = [str(call[0][0]) for call in mock_duckdb.execute.call_args_list]

    assert any("USE test_db" in c for c in sql_calls)
    create_calls = [c for c in sql_calls if "CREATE TABLE" in c]
    assert len(create_calls) > 0
    assert any("test_db.main.test_table" in c for c in create_calls)

    insert_calls = [c for c in sql_calls if "INSERT INTO" in c]
    assert len(insert_calls) > 0
    assert any("test_db.main.test_table" in c for c in insert_calls)
    assert any("rowid BETWEEN" in c for c in insert_calls)

    chunk_calls = [c for c in sql_calls if "rowid BETWEEN" in c]
    assert len(chunk_calls) == 10


def test_validate_data_passes(loader, mock_duckdb):
    mock_duckdb.execute.return_value.fetchone.return_value = (
        1000,  # total_rows
        1000,  # non_null_timestamps
        1000,  # non_null_projects
        "2023-01-01 00:00:00",  # min_ts
        "2023-01-31 23:59:59",  # max_ts
    )

    loader._validate_data(
        timestamp_column="timestamp",
        start_date="2023-01-01",
        end_date="2023-01-31",
    )


def test_validate_data_fails_on_null_timestamps(loader, mock_duckdb):
    mock_duckdb.execute.return_value.fetchone.return_value = (
        1000,
        900,  # 10% null timestamps
        1000,
        "2023-01-01",
        "2023-01-31",
    )

    with pytest.raises(DataQualityError, match="timestamp values are null"):
        loader._validate_data(
            timestamp_column="timestamp",
            start_date="2023-01-01",
            end_date="2023-01-31",
        )


def test_validate_data_fails_on_null_projects(loader, mock_duckdb):
    mock_duckdb.execute.return_value.fetchone.return_value = (
        1000,
        1000,
        800,  # 20% null projects
        "2023-01-01",
        "2023-01-31",
    )

    with pytest.raises(DataQualityError, match="project values are null"):
        loader._validate_data(
            timestamp_column="timestamp",
            start_date="2023-01-01",
            end_date="2023-01-31",
        )


def test_validate_data_fails_on_zero_rows(loader, mock_duckdb):
    mock_duckdb.execute.return_value.fetchone.return_value = (0, 0, 0, None, None)

    with pytest.raises(DataQualityError, match="No rows loaded"):
        loader._validate_data(
            timestamp_column="timestamp",
            start_date="2023-01-01",
            end_date="2023-01-31",
        )
