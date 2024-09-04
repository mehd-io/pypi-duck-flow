import pytest
import pyarrow as pa
import duckdb
from datetime import datetime
from ingestion.motherduck import ArrowTableLoadingBuffer


class TestArrowTableLoadingBufferIntegration:
    @pytest.fixture(autouse=True)
    def setup_in_memory_duckdb(self):
        """Fixture to set up an in-memory DuckDB database for each test."""
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    @pytest.fixture
    def buffer(self, setup_in_memory_duckdb):
        """Fixture to create an ArrowTableLoadingBuffer instance for each test."""
        duckdb_schema = """
        CREATE TABLE IF NOT EXISTS test_table (
            id BIGINT,
            name VARCHAR,
            load_id VARCHAR,
            load_timestamp TIMESTAMP
        );
        """
        pyarrow_schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("load_id", pa.string()),
                pa.field("load_timestamp", pa.timestamp("ns")),
            ]
        )

        buffer = ArrowTableLoadingBuffer(
            duckdb_schema=duckdb_schema,
            pyarrow_schema=pyarrow_schema,
            database_name="test_db",
            table_name="test_table",
            dryrun=False,
            destination="local",
        )
        buffer.conn = setup_in_memory_duckdb  # Use the in-memory connection
        buffer.conn.execute(duckdb_schema)  # Create the table
        return buffer

    def test_flush_after_adding_more_data(self, buffer):
        # Add some data to the buffer
        data1 = pa.Table.from_pydict(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "name": pa.array(["Alice", "Bob"]),
                "load_id": pa.array(["id1", "id2"]),
                "load_timestamp": pa.array(
                    [datetime.now(), datetime.now()], type=pa.timestamp("ns")
                ),
            },
            schema=buffer.pyarrow_schema,
        )

        buffer.insert(data1)

        # Add more data to exceed the threshold
        data2 = pa.Table.from_pydict(
            {
                "id": pa.array([3], type=pa.int64()),
                "name": pa.array(["Charlie"]),
                "load_id": pa.array(["id3"]),
                "load_timestamp": pa.array([datetime.now()], type=pa.timestamp("ns")),
            },
            schema=buffer.pyarrow_schema,
        )

        buffer.insert(data2)
        # should trigger an insert
        buffer.flush()

        # Verify data was flushed into DuckDB
        result = buffer.conn.execute("SELECT * FROM test_table").fetchall()
        assert len(result) == 3  # Expect 3 rows (2 from data1 and 1 from data2)

    def test_no_flush_for_below_threshold(self, buffer):
        # Add data to the buffer but do not exceed the threshold
        data = pa.Table.from_pydict(
            {
                "id": pa.array([1], type=pa.int64()),
                "name": pa.array(["Alice"]),
                "load_id": pa.array(["id1"]),
                "load_timestamp": pa.array([datetime.now()], type=pa.timestamp("ns")),
            },
            schema=buffer.pyarrow_schema,
        )

        buffer.insert(data)

        # Verify data is not flushed yet
        result = buffer.conn.execute("SELECT * FROM test_table").fetchall()
        assert len(result) == 0  # No flush, so expect 0 rows

    def test_flush_on_threshold(self, buffer):
        # Add data to exactly meet the threshold
        rows_to_insert = 10000  # Example threshold value
        data = pa.Table.from_pydict(
            {
                "id": pa.array(range(rows_to_insert), type=pa.int64()),
                "name": pa.array([f"name_{i}" for i in range(rows_to_insert)]),
                "load_id": pa.array([f"id_{i}" for i in range(rows_to_insert)]),
                "load_timestamp": pa.array(
                    [datetime.now() for _ in range(rows_to_insert)],
                    type=pa.timestamp("ns"),
                ),
            },
            schema=buffer.pyarrow_schema,
        )

        buffer.insert(data)

        # Verify data was flushed into DuckDB
        result = buffer.conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
        assert result == rows_to_insert  # Should match number of rows inserted
