SELECT
    CONCAT(date_part('year', date_trunc('month', download_date)), '-', LPAD(CAST(date_part('month', date_trunc('month', download_date)) AS VARCHAR), 2, '0')) AS year_month,
    SUM(daily_download_sum) AS monthly_downloads
FROM
    duckdb_stats.main.pypi_daily_stats
GROUP BY
    date_trunc('month', download_date)
ORDER BY
    date_trunc('month', download_date) DESC
LIMIT 6;
