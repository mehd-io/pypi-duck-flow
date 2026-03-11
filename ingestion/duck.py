import duckdb
import os
from loguru import logger


class DataQualityError(Exception):
    """Raised when data quality checks fail after loading from BigQuery."""

    pass


class MotherDuckBigQueryLoader:
    """
    A specialized loader for transferring data from BigQuery to MotherDuck.

    Uses DuckDB's BigQuery extension with bigquery_scan for optimal performance:
    - Filter pushdown via the Storage Read API (no intermediate temp table in BQ)
    - Parallel read streams (auto-matches DuckDB threads)
    - Arrow-based optimized scan
    """

    def __init__(
        self,
        motherduck_database: str,
        motherduck_table: str,
        project_id: str,
    ):
        self.motherduck_database = motherduck_database
        self.motherduck_table = motherduck_table
        self.project_id = project_id

        self.conn = duckdb.connect(database=":memory:")

        self.conn.execute("INSTALL bigquery FROM community;")
        self.conn.execute("LOAD bigquery;")

        self.conn.execute("SET bq_experimental_filter_pushdown = true")
        self.conn.execute("SET preserve_insertion_order = FALSE")

        if not os.environ.get("motherduck_token"):
            raise ValueError(
                "MotherDuck token is required. Set the environment variable 'MOTHERDUCK_TOKEN'."
            )

        self.conn.execute("ATTACH 'md:'")
        self.conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.motherduck_database}")

    def load_from_bigquery_to_motherduck(
        self,
        table: str,
        filter_str: str,
        columns: list[str],
        timestamp_column: str,
        start_date: str,
        end_date: str,
        chunk_size: int = 100000,
    ) -> int:
        """
        Load data from BigQuery to MotherDuck via a local temp table.

        Returns:
            Total number of rows loaded to MotherDuck
        """
        logger.info("Starting BigQuery to MotherDuck transfer job")

        loaded_rows = self._load_from_bigquery(table, filter_str, columns)

        if loaded_rows == 0:
            logger.warning("No data was loaded from BigQuery")
            return 0

        logger.info(f"Loaded {loaded_rows:,} rows from BigQuery")

        self._validate_data(timestamp_column, start_date, end_date)

        logger.info(f"Deleting existing data for date range {start_date} to {end_date}")
        self._delete_existing_data(timestamp_column, start_date, end_date)

        logger.info("Copying data to MotherDuck")
        copied_rows = self._copy_to_motherduck(chunk_size)

        logger.info(f"Transferred {copied_rows:,} rows to MotherDuck")
        return copied_rows

    def _load_from_bigquery(
        self, table: str, filter_str: str, columns: list[str]
    ) -> int:
        """Load data from BigQuery into a local temp table using bigquery_scan."""
        try:
            columns_sql = ", ".join(columns)
            escaped_filter = filter_str.replace("'", "''")
            query = f"""
                CREATE OR REPLACE TABLE memory.temp_table AS
                SELECT {columns_sql}
                FROM bigquery_scan('{table}',
                    billing_project='{self.project_id}',
                    filter='{escaped_filter}')
            """
            logger.info(f"Running bigquery_scan on {table}")
            self.conn.execute(query)

            result = self.conn.execute(
                "SELECT COUNT(*) FROM memory.temp_table"
            ).fetchone()
            return result[0] if result else 0

        except Exception as e:
            logger.error(f"Error loading data from BigQuery: {e}")
            raise

    def _validate_data(
        self, timestamp_column: str, start_date: str, end_date: str
    ):
        """Run SQL-based data quality checks on the loaded data."""
        stats = self.conn.execute(f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT({timestamp_column}) as non_null_timestamps,
                COUNT(project) as non_null_projects,
                MIN({timestamp_column}) as min_ts,
                MAX({timestamp_column}) as max_ts
            FROM memory.temp_table
        """).fetchone()

        total_rows, non_null_ts, non_null_proj, min_ts, max_ts = stats

        if total_rows == 0:
            raise DataQualityError("No rows loaded from BigQuery")

        null_ts_pct = (total_rows - non_null_ts) / total_rows * 100
        null_proj_pct = (total_rows - non_null_proj) / total_rows * 100

        if null_ts_pct > 5:
            raise DataQualityError(
                f"{null_ts_pct:.1f}% of timestamp values are null (threshold: 5%)"
            )
        if null_proj_pct > 5:
            raise DataQualityError(
                f"{null_proj_pct:.1f}% of project values are null (threshold: 5%)"
            )

        logger.info(
            f"Data quality OK: {total_rows:,} rows, "
            f"timestamp range [{min_ts} to {max_ts}], "
            f"null rates: timestamp={null_ts_pct:.1f}%, project={null_proj_pct:.1f}%"
        )

    def _delete_existing_data(
        self, timestamp_column: str, start_date: str, end_date: str
    ):
        """Delete existing data from MotherDuck in the specified date range."""
        try:
            table_exists = self.conn.execute(f"""
                SELECT COUNT(*) > 0 
                FROM duckdb_tables() 
                WHERE database_name = '{self.motherduck_database}' 
                AND table_name = '{self.motherduck_table}'
            """).fetchone()[0]

            if not table_exists:
                logger.info(
                    f"Table {self.motherduck_database}.main.{self.motherduck_table} "
                    f"does not exist. Skipping delete."
                )
                return

            delete_query = f"""
            DELETE FROM {self.motherduck_database}.main.{self.motherduck_table} 
            WHERE {timestamp_column} >= '{start_date}' AND {timestamp_column} < '{end_date}'
            """
            self.conn.execute(delete_query)
            logger.info(
                f"Deleted existing data for range {start_date} to {end_date}"
            )

        except Exception as e:
            logger.error(f"Error deleting existing data: {e}")
            raise

    def _copy_to_motherduck(self, chunk_size: int) -> int:
        """Copy data from temp table to MotherDuck in chunks."""
        try:
            self.conn.execute(f"USE {self.motherduck_database}")
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS 
                    {self.motherduck_database}.main.{self.motherduck_table} AS 
                SELECT * FROM memory.temp_table LIMIT 0
            """)

            total_rows = self.conn.execute(
                "SELECT COUNT(*) FROM memory.temp_table"
            ).fetchone()[0]

            if total_rows == 0:
                return 0

            logger.info(f"Total rows to copy: {total_rows:,}")

            for chunk_start in range(0, total_rows, chunk_size):
                chunk_end = min(chunk_start + chunk_size - 1, total_rows - 1)
                self.conn.execute(f"""
                    INSERT INTO {self.motherduck_database}.main.{self.motherduck_table}
                    SELECT * FROM memory.temp_table 
                    WHERE rowid BETWEEN {chunk_start} AND {chunk_end}
                """)

                chunk_num = (chunk_start // chunk_size) + 1
                total_chunks = (total_rows + chunk_size - 1) // chunk_size
                logger.info(
                    f"Chunk {chunk_num}/{total_chunks} ({chunk_end - chunk_start + 1} rows)"
                )

            return total_rows

        except Exception as e:
            logger.error(f"Error copying data to MotherDuck: {e}")
            raise
