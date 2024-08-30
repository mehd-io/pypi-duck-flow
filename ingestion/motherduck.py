from loguru import logger
import duckdb
import os
import pyarrow as pa
from typing import ClassVar, Type
import re
from pydantic import BaseModel, Field
from datetime import datetime
import hashlib


def camel_to_snake(name):
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


class SchemaBaseModel(BaseModel):
    load_id: str = None
    load_timestamp: datetime = Field(default_factory=datetime.now)

    duckdb_type_mapping: ClassVar[dict] = {
        datetime: "TIMESTAMP",
        int: "BIGINT",
        float: "DOUBLE",
        str: "VARCHAR",
        bool: "BOOLEAN",
    }

    arrow_type_mapping: ClassVar[dict] = {
        datetime: pa.timestamp("ns"),
        int: pa.int64(),
        float: pa.float64(),
        str: pa.string(),
        bool: pa.bool_(),
    }

    def __init__(self, **data):
        super().__init__(**data)
        if "load_id" not in data or data["load_id"] is None:
            self.load_id = self.generate_load_id()

    @classmethod
    def get_ordered_fields(cls):
        """
        Avoid mismatch between the order of fields in the model and the order of fields in the database.
        """
        return [(name, field) for name, field in cls.__fields__.items()]

    @classmethod
    def get_duckdb_schema(cls, primary_key: str = None) -> str:
        table_name = cls.get_table_name()
        fields = [
            f"{name} {cls.duckdb_type_mapping.get(field.annotation, 'VARCHAR')}"
            for name, field in cls.get_ordered_fields()
        ]
        primary_key_clause = f", PRIMARY KEY ({primary_key})" if primary_key else ""
        query = (
            f"CREATE TABLE IF NOT EXISTS {table_name} (\n    "
            + ",\n    ".join(fields)
            + primary_key_clause
            + "\n)"
        )
        return query

    @classmethod
    def get_pyarrow_schema(cls):
        fields = [
            pa.field(name, cls.arrow_type_mapping.get(field.annotation, pa.string()))
            for name, field in cls.get_ordered_fields()
        ]
        return pa.schema(fields)

    @classmethod
    def get_table_name(cls) -> str:
        return camel_to_snake(cls.__name__)

    def generate_load_id(self, exclude_fields=("load_id", "load_timestamp")):
        hash_input = [
            str(getattr(self, f)) for f in self.__fields__ if f not in exclude_fields
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
