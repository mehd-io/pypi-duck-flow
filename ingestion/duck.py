import duckdb
import os
from loguru import logger


class MotherDuckBigQueryLoader:
    """
    A specialized loader for transferring data from BigQuery to MotherDuck.

    This class abstracts the implementation details of:
    1. Loading data from BigQuery to local DuckDB
    2. Deleting existing data from MotherDuck for the given date range
    3. Loading data in chunks to MotherDuck for optimal performance
    """

    def __init__(
        self,
        motherduck_database: str,
        motherduck_table: str,
        project_id: str,
    ):
        """
        Initialize the MotherDuck BigQuery loader.

        Args:
            motherduck_database: Target MotherDuck database
            motherduck_table: Target MotherDuck table
            project_id: GCP project ID for BigQuery
        """
        self.motherduck_database = motherduck_database
        self.motherduck_table = motherduck_table
        self.project_id = project_id

        # Initialize a single DuckDB connection
        self.conn = duckdb.connect(database=":memory:")

        # Load BigQuery extension
        self.conn.execute("INSTALL bigquery FROM community;")
        self.conn.execute("LOAD bigquery;")

        # Configure BigQuery extension
        self.conn.execute("SET bq_experimental_filter_pushdown = true")
        self.conn.execute(f"SET bq_query_timeout_ms = 300000")  # 5 minutes timeout

        # Attach BigQuery project
        self.conn.execute(
            f"ATTACH 'project={self.project_id}' AS bq (TYPE bigquery, READ_ONLY)"
        )

        # Attach MotherDuck
        if not os.environ.get("motherduck_token"):
            raise ValueError(
                "MotherDuck token is required. Set the environment variable 'MOTHERDUCK_TOKEN'."
            )

        self.conn.execute("ATTACH 'md:'")
        self.conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.motherduck_database}")

    def load_from_bigquery_to_motherduck(
        self,
        query: str,
        timestamp_column: str,
        start_date: str,
        end_date: str,
        chunk_size: int = 100000,  # Optimal MotherDuck chunk size
    ) -> int:
        """
        Load data from BigQuery to MotherDuck.
        Use a temporary table in DuckDB to load the data in chunks.

        Returns:
            Total number of rows loaded to MotherDuck
        """
        logger.info(f"Starting BigQuery to MotherDuck transfer job")

        #  Estimate row count (for progress reporting)
        estimated_rows = self._estimate_row_count(query)
        if estimated_rows > 0:
            logger.info(f"Found approximately {estimated_rows:,} rows to process")

        # STEP 2: Create a DuckDB table with BigQuery data
        logger.info("Loading data from BigQuery to temporary table")
        loaded_rows = self._load_from_bigquery(query)

        if loaded_rows == 0:
            logger.warning("No data was loaded from BigQuery")
            return 0

        logger.info(f"Deleting existing data for date range {start_date} to {end_date}")
        self._delete_existing_data(timestamp_column, start_date, end_date)

        logger.info("Copying data to MotherDuck")
        copied_rows = self._copy_to_motherduck(chunk_size)

        if estimated_rows > 0:
            logger.info(
                f"Transferred {copied_rows:,} rows ({(copied_rows/estimated_rows)*100:.1f}% of estimated)"
            )

        return copied_rows

    def _estimate_row_count(self, query: str) -> int:
        """Estimate the row count for the BigQuery query"""
        try:
            count_query = f"SELECT COUNT(*) FROM bq.({query})"
            result = self.conn.execute(count_query).fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.warning(f"Failed to estimate row count: {e}")
            return 0

    def _load_from_bigquery(self, query: str) -> int:
        """Load data from BigQuery to local temporary table"""
        try:
            # Create a temporary table with the result of the BigQuery query

            query_tmp = f"""
                CREATE OR REPLACE TABLE memory.temp_table AS
                SELECT * FROM bigquery_query('{self.project_id}', '{query}')
            """
            logger.info(query_tmp)
            self.conn.execute(query_tmp)
            # Check row count
            result = self.conn.execute(
                "SELECT COUNT(*) FROM memory.temp_table"
            ).fetchone()
            return result[0] if result else 0

        except Exception as e:
            logger.error(f"Error loading data from BigQuery: {e}")
            raise

    def _delete_existing_data(
        self, timestamp_column: str, start_date: str, end_date: str
    ):
        """Delete existing data from MotherDuck in the specified date range"""
        try:
            # Check if table exists in MotherDuck
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) > 0 
                FROM duckdb_tables() 
                WHERE database_name = '{self.motherduck_database}' 
                AND table_name = '{self.motherduck_table}'
            """).fetchone()[0]

            if not table_exists:
                logger.info(
                    f"Table {self.motherduck_database}.main.{self.motherduck_table} does not exist. Skipping delete operation."
                )
                return

            # Use database in MotherDuck
            delete_query = f"""
            DELETE FROM {self.motherduck_database}.main.{self.motherduck_table} 
            WHERE {timestamp_column} >= '{start_date}' AND {timestamp_column} < '{end_date}'
            """
            self.conn.execute(delete_query)
            logger.info(
                f"Successfully deleted data for date range {start_date} to {end_date}"
            )

        except Exception as e:
            logger.error(f"Error deleting existing data: {e}")
            raise

    def _copy_to_motherduck(self, chunk_size: int) -> int:
        """Copy data from temporary table to MotherDuck in chunks for optimal performance"""
        try:
            # Ensure target table exists in MotherDuck with the same schema
            self.conn.execute(f"USE {self.motherduck_database}")
            logger.info(
                f"Creating table if not exists {self.motherduck_database}.main.{self.motherduck_table}"
            )
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS 
                    {self.motherduck_database}.main.{self.motherduck_table} AS 
                SELECT * FROM memory.temp_table limit 0
            """)

            # Count total rows
            total_rows = self.conn.execute(
                "SELECT COUNT(*) FROM memory.temp_table"
            ).fetchone()[0]
            logger.info(f"Total rows to copy: {total_rows}")
            if total_rows == 0:
                logger.info("No data to copy to MotherDuck")
                return 0

            # Copy data in chunks using rowid for efficient chunking
            for chunk_start in range(0, total_rows, chunk_size):
                chunk_end = min(chunk_start + chunk_size - 1, total_rows - 1)
                logger.info(f"Copying chunk {chunk_start} to {chunk_end}")
                self.conn.execute(f"""
                    INSERT INTO {self.motherduck_database}.main.{self.motherduck_table}
                    SELECT * FROM memory.temp_table 
                    WHERE rowid BETWEEN {chunk_start} AND {chunk_end}
                """)

                chunk_num = (chunk_start // chunk_size) + 1
                total_chunks = (total_rows + chunk_size - 1) // chunk_size
                logger.info(
                    f"Copied chunk {chunk_num}/{total_chunks} ({chunk_end-chunk_start+1} rows)"
                )

            return total_rows

        except Exception as e:
            logger.error(f"Error copying data to MotherDuck: {e}")
            raise
