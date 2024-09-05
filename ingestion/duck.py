""" Helper functions for interacting with DuckDB and MotherDUck """
import duckdb
import os
import pyarrow as pa
from loguru import logger


class ArrowTableLoadingBuffer:
    def __init__(
        self,
        duckdb_schema: str,
        pyarrow_schema: pa.Schema,
        database_name: str,
        table_name: str,
        dryrun: bool = False,
        destination="local",
        flush_threshold: int = 10000,
    ):
        self.duckdb_schema = duckdb_schema
        self.pyarrow_schema = pyarrow_schema
        self.dryrun = dryrun
        self.database_name = database_name
        self.table_name = table_name
        self.accumulated_data = pa.Table.from_batches([], schema=pyarrow_schema)
        self.total_inserted = 0
        self.conn = self.initialize_connection(destination, duckdb_schema)
        self.primary_key_exists = "PRIMARY KEY" in duckdb_schema.upper()
        self.flush_threshold = flush_threshold

    def initialize_connection(self, destination, sql):
        if destination == "md":
            logger.info("Connecting to MotherDuck...")
            if not os.environ.get("motherduck_token"):
                raise ValueError(
                    "MotherDuck token is required. Set the environment variable 'MOTHERDUCK_TOKEN'."
                )
            conn = duckdb.connect("md:")
            if not self.dryrun:
                logger.info(
                    f"Creating database {self.database_name} if it doesn't exist"
                )
                conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
                conn.execute(f"USE {self.database_name}")
        else:
            conn = duckdb.connect(database=f"{self.database_name}.db")
        if not self.dryrun:
            conn.execute(sql)
        return conn

    def insert(self, table: pa.Table):
        if self.accumulated_data is None or len(self.accumulated_data) == 0:
            self.accumulated_data = table
        else:
            # Ensure the schemas match before concatenating
            target_schema = self.accumulated_data.schema
            table = table.select(target_schema.names)
            self.accumulated_data = pa.concat_tables([self.accumulated_data, table])

        # Check if we need to flush
        if self.accumulated_data.num_rows >= self.flush_threshold:
            logger.info(
                f"Flushing {self.accumulated_data.num_rows} records to the database."
            )
            self.flush()

    def flush_if_needed(self):
        if self.accumulated_data.num_rows >= self.flush_threshold:
            self.flush()

    def flush(self) -> None:
        if not self.dryrun and self.accumulated_data.num_rows > 0:
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

    def load_aws_credentials(self, profile: str):
        logger.info(f"Loading AWS credentials for profile: {profile}")
        self.conn.sql(f"CALL load_aws_credentials('{profile}');")

    def write_to_s3(self, s3_path: str, timestamp_column: str, aws_profile: str):
        self.load_aws_credentials(aws_profile)
        logger.info(f"Writing data to S3 {s3_path}/{self.table_name}")
        self.conn.sql(
            f"""
            COPY (
                SELECT *,
                    YEAR({timestamp_column}) AS year, 
                    MONTH({timestamp_column}) AS month 
                FROM {self.table_name}
            ) 
            TO '{s3_path}/{self.table_name}' 
            (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
            """
        )
