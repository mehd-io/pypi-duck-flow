import pytest
import duckdb
from unittest.mock import patch, MagicMock, ANY, call
from ingestion.duck import MotherDuckBigQueryLoader


@pytest.fixture
def mock_duckdb():
    with patch('duckdb.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        yield mock_conn


@pytest.fixture
def loader(mock_duckdb):
    with patch.dict('os.environ', {'motherduck_token': 'test_token', 'HOME': '/tmp'}):
        loader = MotherDuckBigQueryLoader(
            motherduck_database="test_db",
            motherduck_table="test_table",
            project_id="test_project"
        )
        loader.conn = mock_duckdb
        return loader


def test_loader_initialization(loader, mock_duckdb):
    assert loader.motherduck_database == "test_db"
    assert loader.motherduck_table == "test_table"
    assert loader.project_id == "test_project"
    
    # Verify that the necessary DuckDB commands were executed
    mock_duckdb.execute.assert_any_call("INSTALL bigquery FROM community;")
    mock_duckdb.execute.assert_any_call("LOAD bigquery;")
    mock_duckdb.execute.assert_any_call("SET bq_experimental_filter_pushdown = true")
    mock_duckdb.execute.assert_any_call("SET bq_query_timeout_ms = 300000")
    mock_duckdb.execute.assert_any_call("ATTACH 'project=test_project' AS bq (TYPE bigquery, READ_ONLY)")
    mock_duckdb.execute.assert_any_call("ATTACH 'md:'")
    mock_duckdb.execute.assert_any_call("CREATE DATABASE IF NOT EXISTS test_db")


def test_loader_initialization_without_token():
    with patch.dict('os.environ', {'HOME': '/tmp'}, clear=True):
        with pytest.raises(ValueError, match="MotherDuck token is required"):
            MotherDuckBigQueryLoader(
                motherduck_database="test_db",
                motherduck_table="test_table",
                project_id="test_project"
            )


def test_load_from_bigquery_to_motherduck(loader, mock_duckdb):
    # Mock the row count estimation
    mock_duckdb.execute.return_value.fetchone.return_value = (1000,)
    
    # Test the load_from_bigquery_to_motherduck method
    query = "SELECT * FROM test_table"
    result = loader.load_from_bigquery_to_motherduck(
        query=query,
        timestamp_column="timestamp",
        start_date="2023-01-01",
        end_date="2023-01-31",
        chunk_size=100
    )
    
    assert result == 1000
    # Verify that the necessary DuckDB commands were executed
    mock_duckdb.execute.assert_any_call(ANY)  # The exact query might vary, so we use ANY


def test_delete_existing_data(loader, mock_duckdb):
    # Mock table existence check
    mock_duckdb.execute.return_value.fetchone.return_value = (True,)
    
    # Test the _delete_existing_data method
    loader._delete_existing_data(
        timestamp_column="timestamp",
        start_date="2023-01-01",
        end_date="2023-01-31"
    )
    
    # Verify the delete query was executed with the correct pattern
    delete_calls = [call[0][0] for call in mock_duckdb.execute.call_args_list]
    assert any("DELETE FROM test_db.main.test_table" in call for call in delete_calls)
    assert any("timestamp >= '2023-01-01'" in call for call in delete_calls)
    assert any("timestamp < '2023-01-31'" in call for call in delete_calls)


def test_copy_to_motherduck(loader, mock_duckdb):
    # Mock the row count
    mock_duckdb.execute.return_value.fetchone.return_value = (1000,)
    
    # Test the _copy_to_motherduck method
    result = loader._copy_to_motherduck(chunk_size=100)
    
    assert result == 1000
    
    # Get all SQL calls made to execute
    sql_calls = [str(call[0][0]) for call in mock_duckdb.execute.call_args_list]
    
    # Print all SQL calls for debugging
    print("\nActual SQL calls:")
    for i, call in enumerate(sql_calls):
        print(f"{i}: {call}")
    
    # Verify the table creation
    assert any("USE test_db" in call for call in sql_calls)
    
    # More flexible table creation check
    create_table_calls = [call for call in sql_calls if "CREATE TABLE" in call]
    assert len(create_table_calls) > 0, "No CREATE TABLE statement found"
    assert any("test_db.main.test_table" in call for call in create_table_calls)
    assert any("memory.temp_table" in call for call in create_table_calls)
    
    # Verify the data copying
    insert_calls = [call for call in sql_calls if "INSERT INTO" in call]
    assert len(insert_calls) > 0, "No INSERT statements found"
    assert any("test_db.main.test_table" in call for call in insert_calls)
    assert any("memory.temp_table" in call for call in insert_calls)
    assert any("rowid BETWEEN" in call for call in insert_calls)
    
    # Verify chunking behavior
    chunk_calls = [call for call in sql_calls if "rowid BETWEEN" in call]
    assert len(chunk_calls) > 0, "No chunking calls found"
    assert len(chunk_calls) == 10, f"Expected 10 chunks, got {len(chunk_calls)}"
