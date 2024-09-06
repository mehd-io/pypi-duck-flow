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
        chunk_size: int = 20000,
    ):
        self.duckdb_schema = duckdb_schema
        self.pyarrow_schema = pyarrow_schema
        self.dryrun = dryrun
        self.database_name = database_name
        self.table_name = table_name
        self.total_inserted = 0
        self.conn = self.initialize_connection(destination, duckdb_schema)
        self.primary_key_exists = "PRIMARY KEY" in duckdb_schema.upper()
        self.chunk_size = chunk_size

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
        if not self.dryrun:
            total_rows = table.num_rows
            for batch_start in range(0, total_rows, self.chunk_size):
                batch_end = min(batch_start + self.chunk_size, total_rows)
                chunk = table.slice(batch_start, batch_end - batch_start)
                self.insert_chunk(chunk)
                logger.info(f"Inserted chunk {batch_start} to {batch_end}")
            self.total_inserted += total_rows
            logger.info(f"Total inserted: {self.total_inserted} rows")

    def insert_chunk(self, chunk: pa.Table):
        self.conn.register("buffer_table", chunk)
        if self.primary_key_exists:
            insert_query = f"""
            INSERT OR REPLACE INTO {self.table_name} SELECT * FROM buffer_table
            """
        else:
            insert_query = f"INSERT INTO {self.table_name} SELECT * FROM buffer_table"
        self.conn.execute(insert_query)
        self.conn.unregister("buffer_table")

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
