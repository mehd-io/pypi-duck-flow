import pyarrow as pa
import pytest
from ingestion.duck import ArrowTableLoadingBuffer


@pytest.fixture
def buffer_factory():
    def _create_buffer(schema, primary_key=False):
        pk_clause = "PRIMARY KEY" if primary_key else ""
        return ArrowTableLoadingBuffer(
            duckdb_schema=f"CREATE TABLE test_table (id INTEGER {pk_clause}, name VARCHAR)",
            pyarrow_schema=pa.schema([("id", pa.int64()), ("name", pa.string())]),
            database_name=":memory:",
            table_name="test_table",
            dryrun=False,
            destination="local",
            chunk_size=2,  # Small chunk size for testing
        )

    return _create_buffer


def test_insert_and_query(buffer_factory):
    buffer = buffer_factory(schema="CREATE TABLE test_table (id INTEGER, name VARCHAR)")
    data = pa.Table.from_pydict(
        {
            "id": pa.array([1, 2, 3, 4], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie", "David"]),
        }
    )
    buffer.insert(data)

    # Check total number of rows
    total_rows = buffer.conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
    assert total_rows == 4, f"Expected 4 total rows, but found {total_rows}"

    # Verify that all data was inserted correctly
    result = buffer.conn.execute(
        "SELECT id, name FROM test_table ORDER BY id"
    ).fetchall()
    expected = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")]
    assert result == expected, "Data in the table does not match expected values"


def test_insert_chunks(buffer_factory):
    buffer = buffer_factory(schema="CREATE TABLE test_table (id INTEGER, name VARCHAR)")
    chunk1 = pa.Table.from_pydict(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["Alice", "Bob"]),
        }
    )
    chunk2 = pa.Table.from_pydict(
        {
            "id": pa.array([3, 4], type=pa.int64()),
            "name": pa.array(["Charlie", "David"]),
        }
    )

    buffer.insert(chunk1)
    buffer.insert(chunk2)

    # Check total number of rows
    total_rows = buffer.conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
    assert total_rows == 4, f"Expected 4 total rows, but found {total_rows}"

    # Verify that all data was inserted correctly
    result = buffer.conn.execute(
        "SELECT id, name FROM test_table ORDER BY id"
    ).fetchall()
    expected = [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")]
    assert result == expected, "Data in the table does not match expected values"


def test_insert_large_table(buffer_factory):
    buffer = buffer_factory(schema="CREATE TABLE test_table (id INTEGER, name VARCHAR)")
    large_data = pa.Table.from_pydict(
        {
            "id": pa.array(range(1, 101), type=pa.int64()),
            "name": pa.array([f"Name_{i}" for i in range(1, 101)]),
        }
    )
    buffer.insert(large_data)

    # Check total number of rows
    total_rows = buffer.conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
    assert total_rows == 100, f"Expected 100 total rows, but found {total_rows}"

    # Verify first and last rows
    first_row = buffer.conn.execute(
        "SELECT id, name FROM test_table ORDER BY id LIMIT 1"
    ).fetchone()
    last_row = buffer.conn.execute(
        "SELECT id, name FROM test_table ORDER BY id DESC LIMIT 1"
    ).fetchone()

    assert first_row == (1, "Name_1"), f"First row does not match expected: {first_row}"
    assert last_row == (
        100,
        "Name_100",
    ), f"Last row does not match expected: {last_row}"


def test_primary_key_handling(buffer_factory):
    buffer = buffer_factory(
        schema="CREATE TABLE test_table (id INTEGER PRIMARY KEY, name VARCHAR)",
        primary_key=True,
    )

    # Insert initial data
    initial_data = pa.Table.from_pydict(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["Alice", "Bob"]),
        }
    )
    buffer.insert(initial_data)

    # Insert data with a duplicate key
    duplicate_data = pa.Table.from_pydict(
        {
            "id": pa.array([2, 3], type=pa.int64()),
            "name": pa.array(["Updated Bob", "Charlie"]),
        }
    )
    buffer.insert(duplicate_data)

    # Check total number of rows (should be 3, not 4)
    total_rows = buffer.conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
    assert total_rows == 3, f"Expected 3 total rows, but found {total_rows}"

    # Verify that the duplicate key was updated
    result = buffer.conn.execute(
        "SELECT id, name FROM test_table ORDER BY id"
    ).fetchall()
    expected = [(1, "Alice"), (2, "Updated Bob"), (3, "Charlie")]
    assert result == expected, "Data in the table does not match expected values"
