import pyarrow as pa
import pytest


@pytest.fixture
def buffer():
    # Create and return a buffer instance for testing
    # You may need to adjust this based on your actual ArrowTableLoadingBuffer initialization
    from ingestion.duck import ArrowTableLoadingBuffer

    return ArrowTableLoadingBuffer(
        duckdb_schema="CREATE TABLE test_table (id INTEGER, name VARCHAR)",
        pyarrow_schema=pa.schema([("id", pa.int64()), ("name", pa.string())]),
        database_name=":memory:",
        table_name="test_table",
        dryrun=False,
        destination="local",
        flush_threshold=1000,
    )


def test_insert_and_flush(buffer):
    data1 = pa.Table.from_pydict(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["Alice", "Bob"]),
        }
    )
    buffer.insert(data1)
    buffer.flush()
    data2 = pa.Table.from_pydict(
        {
            "id": pa.array([3, 4], type=pa.int64()),
            "name": pa.array(["Charlie", "David"]),
        }
    )
    buffer.insert(data2)
    buffer.flush()

    # Check total number of rows
    total_rows = buffer.conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
    assert total_rows == 4, f"Expected 4 total rows, but found {total_rows}"

    # Verify that all data was inserted correctly
    result = buffer.conn.execute(
        "SELECT id, name FROM test_table ORDER BY id"
    ).fetchall()
    expected = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")]
    assert result == expected, "Data in the table does not match expected values"

    # Insert duplicate data
    buffer.insert(data1)
    buffer.flush()

    # Check total number of rows after inserting duplicates
    total_rows_after_duplicate = buffer.conn.execute(
        "SELECT COUNT(*) FROM test_table"
    ).fetchone()[0]
    expected_rows_after_duplicate = 6 if not buffer.primary_key_exists else 4
    assert (
        total_rows_after_duplicate == expected_rows_after_duplicate
    ), f"Expected {expected_rows_after_duplicate} rows after duplicate insert, but found {total_rows_after_duplicate}"
