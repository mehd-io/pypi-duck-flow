""" Helper functions for interacting with DuckDB """
from loguru import logger
import pandas as pd


def create_table_from_dataframe(
    duckdb_con, table_name: str, dataframe: str, table_ddl: str
):
    logger.info(f"Creating table {table_name} in local DuckDB")
    duckdb_con.sql(table_ddl)
    logger.info("inserting data into table")
    duckdb_con.sql(
        f"""
        INSERT INTO {table_name} 
            SELECT *
            FROM {dataframe}
        """
    )


def connect_to_md(duckdb_con, motherduck_token: str):
    duckdb_con.sql(f"INSTALL md;")
    duckdb_con.sql(f"LOAD md;")
    duckdb_con.sql(f"SET motherduck_token='{motherduck_token}';")
    duckdb_con.sql(f"ATTACH 'md:'")


def load_aws_credentials(duckdb_con, profile: str):
    duckdb_con.sql(f"CALL load_aws_credentials('{profile}');")


def write_to_s3_from_duckdb(
    duckdb_con, table: str, s3_path: str, timestamp_column: str
):
    logger.info(f"Writing data to S3 {s3_path}/{table}")
    duckdb_con.sql(
        f"""
        COPY (
            SELECT *,
                YEAR({timestamp_column}) AS year, 
                MONTH({timestamp_column}) AS month 
            FROM {table}
        ) 
        TO '{s3_path}/{table}' 
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
    """
    )


def write_to_md_from_duckdb(
    duckdb_con,
    table: str,
    local_database: str,
    remote_database: str,
    timestamp_column: str,
    start_date: str,
    end_date: str,
):
    logger.info(
        f"Creating database {remote_database} from {local_database}.{table} if it doesn't exist"
    )
    duckdb_con.sql(f"CREATE DATABASE IF NOT EXISTS {remote_database}")
    logger.info(
        f"Creating table {remote_database}.main.{table} from {local_database}.{table} if it doesn't exist"
    )
    duckdb_con.sql(
        f"CREATE TABLE IF NOT EXISTS {remote_database}.{table} AS SELECT * FROM {local_database}.{table} limit 0"
    )
    # Delete any existing data in the date range
    logger.info(f"Deleting data from {start_date} to {end_date}")
    duckdb_con.sql(
        f"DELETE FROM {remote_database}.main.{table} WHERE {timestamp_column} BETWEEN '{start_date}' AND '{end_date}'"
    )
    # Insert new data
    logger.info(f"Writing data to motherduck {remote_database}.main.{table}")
    duckdb_con.sql(
        f"""
    INSERT INTO {remote_database}.main.{table}
    SELECT *
        FROM {local_database}.{table}"""
    )
