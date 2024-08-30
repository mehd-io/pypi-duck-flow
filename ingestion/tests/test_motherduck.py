import pytest
from datetime import datetime
import pyarrow as pa
from ingestion.motherduck import SchemaBaseModel


# Unit test for SchemaBaseModel
class TestSchemaBaseModel:
    def test_generate_load_id(self):
        # Define a simple model extending SchemaBaseModel
        class MyModel(SchemaBaseModel):
            my_field: str

        # Create an instance of the model
        model_instance = MyModel(my_field="test_value")

        # Generate the load_id
        generated_id = model_instance.generate_load_id()

        # Check that the load_id is not None and is a string of length 64 (SHA-256 hash)
        assert generated_id is not None
        assert isinstance(generated_id, str)
        assert len(generated_id) == 64

    def test_get_table_name(self):
        class MyModel(SchemaBaseModel):
            my_field: str

        assert MyModel.get_table_name() == "my_model"

    def test_get_duckdb_schema(self):
        class MyModel(SchemaBaseModel):
            my_field: str
            my_int: int

        expected_schema = (
            "CREATE TABLE IF NOT EXISTS my_model (\n    "
            "load_id VARCHAR,\n    "
            "load_timestamp TIMESTAMP,\n    "
            "my_field VARCHAR,\n    "
            "my_int BIGINT, PRIMARY KEY (load_id)\n)"
        )
        assert MyModel.get_duckdb_schema(primary_key="load_id") == expected_schema

    def test_get_pyarrow_schema(self):
        class MyModel(SchemaBaseModel):
            my_field: str
            my_int: int

        expected_schema = pa.schema(
            [
                pa.field("load_id", pa.string()),
                pa.field("load_timestamp", pa.timestamp("ns")),
                pa.field("my_field", pa.string()),
                pa.field("my_int", pa.int64()),
            ]
        )
        assert MyModel.get_pyarrow_schema() == expected_schema
