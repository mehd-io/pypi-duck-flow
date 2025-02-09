# Changelog

## 2025-02-09
- Bumping DuckDB to `1.2.0`.

## 2024-11-29
- **Changed** Change to `uv` for packaging and upgrade Python `3.12` and duckdb to `1.1.3`.

## 2024-10-21
- **Added**: New charts on Evidence dashboard and source file [`downloads_by_version.sql`](https://github.com/mehd-io/pypi-duck-flow/blob/main/dashboard/sources/motherduck/downloads_by_version.sql).

## 2024-09-26
- **Changed**: Made `ArrowTableLoadingBuffer` more flexible to process larger datasets and updated the chunk size to optimize processing.

## 2024-09-19
- **Changed**: Improved MotherDuck share updates and github action workflows.

## 2024-09-06
- **Added**: New helper [`ArrowTableLoadingBuffer`](https://github.com/mehd-io/pypi-duck-flow/blob/main/ingestion/duck.py#L7) for optimizing ingestion process.
- **Changed** Use pyarrow for ingestion from [BigQuery](https://github.com/mehd-io/pypi-duck-flow/blob/main/ingestion/bigquery.py#L69) and updated pydantic model [FileDownloads](https://github.com/mehd-io/pypi-duck-flow/blob/main/ingestion/models.py#L81).
- **Changed**: Injected database name dynamically in the dbt project.
