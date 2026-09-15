# Changelog

## 2026-09-15
- **Changed**: Recalculate the complete PyPI history using distribution artifacts (`.whl`, `.tar.gz`, and `.zip`) so counts remain comparable across PyPI's August 24, 2026 logging change.
- Preserve the broader pre-change definition as `legacy_daily_download_sum` for transparent comparison.
- Add a dashboard-wide "Comparable download history" switch, enabled by default, with methodology guidance and a link to the [official PyPI announcement](https://blog.pypi.org/posts/2026-08-31-download-counts/).
- Add an isolated MotherDuck preview target that rebuilds transformed data from the public share without repeating the BigQuery ingestion.
- Make the dashboard MotherDuck database configurable with `MOTHERDUCK_DATABASE_PATH`.

## 2026-03-25
- Bump DuckDB to `1.5.1` (pyproject.toml + GitHub Actions setup-duckdb).
- Upgrade GitHub Actions to Node.js 24-compatible versions: `actions/checkout@v6`, `google-github-actions/auth@v3`.

## 2026-03-12
- **Changed**: Rewrite `/dashboard` from Evidence.dev (SQL + Markdown) to Next.js + TypeScript + Tailwind CSS + shadcn/ui.
- New stack: Next.js App Router, Recharts via shadcn/ui chart components, `@duckdb/node-api` for MotherDuck connectivity.
- KPI cards with sparklines and week/month trend indicators.
- New daily downloads bar chart.
- Time period filter (All time, 7d, 30d, 90d) for breakdown charts.
- Version adoption area chart with absolute/% share toggle.
- Dark/light mode support, system font stack.
- MotherDuck share link copy-to-clipboard button in header.
- Deployable on Vercel with `MOTHERDUCK_TOKEN` env var.

## 2026-03-11
- Rework `/ingestion` to use the DuckDB BigQuery extension with `bigquery_scan` and filter pushdown for faster ingestion (replaces BigQuery Python API and `bigquery_query()`).
- Add SQL-based data quality checks after BigQuery load (null rate thresholds on `timestamp` and `project` columns).
- Bump DuckDB to `1.4.4`, dbt-duckdb to `1.10.1`.
- Remove `db-dtypes`, `google-cloud-bigquery`, `google-auth`, `pyarrow` dependencies.
- Clean up obsolete `s3_path` / `aws_profile` parameters from Makefile and GitHub Actions.

## 2025-10-10
- Bumping DuckDB==1.4.1, pyarrow==21.0.0, dbt-duckdb==1.9.6

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
