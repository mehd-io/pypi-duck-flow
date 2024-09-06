{% set database_name = var('database_name', 'duckdb_stats') %}

WITH pre_aggregated_data AS (
    SELECT
        timestamp :: date as download_date,
        details.system.name AS system_name,
        details.system.release AS system_release,
        file.version AS version,
        project,
        country_code,
        details.cpu,
        CASE
            WHEN details.python IS NULL THEN NULL
            ELSE CONCAT(
                SPLIT_PART(details.python, '.', 1),
                '.',
                SPLIT_PART(details.python, '.', 2)
            )
        END AS python_version
    FROM
        {{ dbt_unit_testing.source(
            'external_source' if var('data_source') == 'external_source' else database_name,
            'pypi_file_downloads'
        )}}
    WHERE
        download_date >= '{{ var("start_date") }}'
        AND download_date < '{{ var("end_date") }}'
)

SELECT
    MD5(CONCAT_WS('|', download_date, system_name, system_release, version, project, country_code, cpu, python_version)) AS load_id,
    download_date,
    system_name,
    system_release,
    version,
    project,
    country_code,
    cpu,
    python_version,
    COUNT(*) AS daily_download_sum
FROM
    pre_aggregated_data
GROUP BY
    ALL
