import pytest
import pyarrow as pa
import duckdb
from datetime import datetime
from ingestion.duck import ArrowTableLoadingBuffer

class TestArrowTableLoadingBufferIntegration:
    @pytest.fixture(autouse=True)
    def setup_in_memory_duckdb(self):
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    @pytest.fixture
    def buffer(self, setup_in_memory_duckdb):
        duckdb_schema = """
        CREATE TABLE IF NOT EXISTS test_table (
            id BIGINT,
            name VARCHAR,
            load_id VARCHAR,
            load_timestamp TIMESTAMP
        );
        """
        pyarrow_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])

        buffer = ArrowTableLoadingBuffer(
            duckdb_schema=duckdb_schema,
            pyarrow_schema=pyarrow_schema,
            database_name="test_db",
            table_name="test_table",
            dryrun=False,
            destination="local",
            flush_threshold=5,  # Lower threshold for testing
        )
        buffer.conn = setup_in_memory_duckdb
        buffer.conn.execute(duckdb_schema)
        return buffer

    @pytest.fixture
    def sample_data(self):
        return pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["Alice", "Bob"]),
        })

    def test_generate_load_id(self, buffer, sample_data):
        load_id = buffer._generate_load_id(sample_data)
        assert isinstance(load_id, str)
        assert len(load_id) == 64

    def test_add_load_metadata(self, buffer, sample_data):
        table_with_metadata = buffer._add_load_metadata(sample_data)
        assert 'load_id' in table_with_metadata.column_names
        assert 'load_timestamp' in table_with_metadata.column_names
        assert len(table_with_metadata) == len(sample_data)
        assert all(table_with_metadata['load_id'][0] == row for row in table_with_metadata['load_id'])
        assert all(table_with_metadata['load_timestamp'][0] == row for row in table_with_metadata['load_timestamp'])

    def test_insert_with_metadata(self, buffer, sample_data):
        buffer.insert(sample_data)
        assert buffer.accumulated_data is not None
        assert 'load_id' in buffer.accumulated_data.column_names
        assert 'load_timestamp' in buffer.accumulated_data.column_names
        assert len(buffer.accumulated_data) == len(sample_data)

    def test_flush_after_adding_more_data(self, buffer):
        data1 = pa.Table.from_pydict({
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie"]),
        })
        buffer.insert(data1)
        data2 = pa.Table.from_pydict({
            "id": pa.array([4, 5], type=pa.int64()),
            "name": pa.array(["David", "Eve"]),
        })
        buffer.insert(data2)
        buffer.flush()
        result = buffer.conn.execute("SELECT * FROM test_table").fetchall()
        assert len(result) == 5
        assert all(row[2] is not None for row in result)
        assert all(row[3] is not None for row in result)

    def test_no_flush_for_below_threshold(self, buffer):
        data = pa.Table.from_pydict({
            "id": pa.array([1], type=pa.int64()),
            "name": pa.array(["Alice"]),
        })
        buffer.insert(data)
        result = buffer.conn.execute("SELECT * FROM test_table").fetchall()
        assert len(result) == 0

    def test_flush_on_threshold(self, buffer):
        data = pa.Table.from_pydict({
            "id": pa.array(range(5), type=pa.int64()),
            "name": pa.array([f"name_{i}" for i in range(5)]),
        })
        buffer.insert(data)
        result = buffer.conn.execute("SELECT COUNT(*) FROM test_table").fetchone()[0]
        assert result == 5

    def test_consistent_load_id_within_batch(self, buffer):
        data1 = pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["Alice", "Bob"]),
        })
        buffer.insert(data1)
        data2 = pa.Table.from_pydict({
            "id": pa.array([3, 4], type=pa.int64()),
            "name": pa.array(["Charlie", "David"]),
        })
        buffer.insert(data2)
        buffer.flush()
        result = buffer.conn.execute("SELECT DISTINCT load_id FROM test_table").fetchall()
        assert len(result) == 1

    def test_different_load_id_between_batches(self, buffer):
        data1 = pa.Table.from_pydict({
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["Alice", "Bob"]),
        })
        buffer.insert(data1)
        buffer.flush()
        data2 = pa.Table.from_pydict({
            "id": pa.array([3, 4], type=pa.int64()),
            "name": pa.array(["Charlie", "David"]),
        })
        buffer.insert(data2)
        buffer.flush()
        result = buffer.conn.execute("SELECT DISTINCT load_id FROM test_table").fetchall()
        assert len(result) == 2
