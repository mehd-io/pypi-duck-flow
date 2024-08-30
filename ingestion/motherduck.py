from loguru import logger
import duckdb
import os
import pyarrow as pa
from pydantic import BaseModel, Field
from datetime import datetime
import hashlib
import re
from typing import ClassVar, Dict, Any, Type, Optional, get_origin

def camel_to_snake(name: str) -> str:
    """Converts CamelCase to snake_case."""
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()

class SchemaBaseModel(BaseModel):
    load_id: str = Field(default=None, alias="load_id")
    load_timestamp: datetime = Field(default_factory=datetime.now, alias="load_timestamp")

    duckdb_type_mapping: ClassVar[Dict[Any, str]] = {
        datetime: "TIMESTAMP",
        int: "BIGINT",
        float: "DOUBLE",
        str: "VARCHAR",
        bool: "BOOLEAN",
        Optional[datetime]: "TIMESTAMP",
        Optional[int]: "BIGINT",
        Optional[float]: "DOUBLE",
        Optional[str]: "VARCHAR",
        Optional[bool]: "BOOLEAN",
    }

    arrow_type_mapping: ClassVar[Dict[Any, Any]] = {
        datetime: pa.timestamp("ns"),
        int: pa.int64(),
        float: pa.float64(),
        str: pa.string(),
        bool: pa.bool_(),
        Optional[datetime]: pa.timestamp("ns"),
        Optional[int]: pa.int64(),
        Optional[float]: pa.float64(),
        Optional[str]: pa.string(),
        Optional[bool]: pa.bool_(),
    }

    def __init__(self, **data):
        super().__init__(**data)
        if not self.load_id:
            self.load_id = self.generate_load_id()

    @classmethod
    def get_ordered_fields(cls):
        """Returns ordered fields to ensure consistency between model and DB schema."""
        return [(name, field) for name, field in cls.model_fields.items()]

    @classmethod
    def get_duckdb_schema(cls, primary_key: str = None) -> str:
        """Generates the DuckDB table schema."""
        table_name = cls.get_table_name()
        fields = []
        for name, field in cls.model_fields.items():
            field_type = cls.duckdb_type_mapping.get(
                get_origin(field.annotation) or field.annotation, 'VARCHAR'
            )
            if field.alias is None:
                field_alias = name
            else:
                field_alias = field.alias
            fields.append(f'"{field_alias}" {field_type}')
        primary_key_clause = f', PRIMARY KEY ("{primary_key}")' if primary_key else ""
        return (
            f'CREATE TABLE IF NOT EXISTS {table_name} (\n    '
            + ",\n    ".join(fields)
            + primary_key_clause
            + "\n)"
        )

    @classmethod
    def get_pyarrow_schema(cls):
        """Generates the PyArrow schema."""
        fields = []
        for name, field in cls.model_fields.items():
            field_type = cls.arrow_type_mapping.get(
                get_origin(field.annotation) or field.annotation, pa.string()
            )
            if field_type is None:
                field_type = pa.string()  # Fallback to pa.string()
            if field.alias is None:
                field_alias = name
            else:
                field_alias = field.alias
            fields.append(pa.field(field_alias, field_type))
        return pa.schema(fields)

    @classmethod
    def get_table_name(cls) -> str:
        """Returns the table name derived from the model's class name."""
        return camel_to_snake(cls.__name__)

    def generate_load_id(self, exclude_fields=("load_id", "load_timestamp")) -> str:
        """Generates a unique load ID based on the model's fields."""
        hash_input = [
            str(getattr(self, field)) for field in self.model_fields if field not in exclude_fields
        ]
        row_str = "".join(hash_input)
        return hashlib.sha256(row_str.encode("utf-8")).hexdigest()

class ArrowTableLoadingBuffer:
    """
    Buffer for loading into DuckDB via Arrow tables from Pydantic models
    """

    def __init__(
        self,
        model: Type[SchemaBaseModel],
        database_name: str,
        dryrun: bool = False,
        destination="local",
        primary_key: str = "load_id",
    ):
        self.model = model
        self.dryrun = dryrun
        self.database_name = database_name
        self.table_name = model.get_table_name()
        self.accumulated_data = pa.Table.from_batches(
            [], schema=model.get_pyarrow_schema()
        )
        self.total_inserted = 0

        duckdb_schema = model.get_duckdb_schema(primary_key=primary_key)
        self.conn = self.initialize_connection(destination, duckdb_schema)
        self.primary_key_exists = "PRIMARY KEY" in duckdb_schema.upper()

    def initialize_connection(self, destination, sql):
        if destination == "md":
            logger.info("Connecting to MotherDuck...")
            if not os.environ.get("motherduck_token"):
                raise ValueError(
                    "MotherDuck token is required. Set the environment variable 'MOTHERDUCK_TOKEN'."
                )
            conn = duckdb.connect("md:")
            conn.execute(f"USE {self.database_name}")

            if not self.dryrun:
                logger.info(
                    f"Creating database {self.database_name} if it doesn't exist"
                )
                conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
        else:
            conn = duckdb.connect(database=f"{self.database_name}.db")

        if not self.dryrun:
            conn.execute(sql)
        return conn

    def insert(self, table: pa.Table) -> None:
        self.accumulated_data = (
            pa.concat_tables([self.accumulated_data, table])
            if self.accumulated_data.num_rows > 0
            else table
        )
        self.flush_if_needed()

    def insert_from_model(self, model_instances: list[SchemaBaseModel]) -> None:
        data_list = [instance.model_dump() for instance in model_instances]
        self.insert_from_pylist(data_list)

    def insert_from_pylist(self, data_list: list[dict]) -> None:
        table = pa.Table.from_pylist(data_list, schema=self.accumulated_data.schema)
        self.insert(table)

    def flush_if_needed(self):
        if self.accumulated_data.num_rows >= 10000:
            self.flush()

    def flush(self) -> None:
        if not self.dryrun:
            self.conn.register("buffer_table", self.accumulated_data)
            if self.primary_key_exists:
                insert_query = f"""
                INSERT OR REPLACE INTO {self.table_name} SELECT * FROM buffer_table
                """
            else:
                insert_query = (
                    f"INSERT INTO {self.table_name} SELECT * FROM buffer_table"
                )

            self.conn.execute(insert_query)
            self.conn.unregister("buffer_table")
            self.total_inserted += self.accumulated_data.num_rows
            logger.info(
                f"Flushed {self.accumulated_data.num_rows} records to the database."
            )

            self.accumulated_data = pa.Table.from_batches(
                [], schema=self.accumulated_data.schema
            )
