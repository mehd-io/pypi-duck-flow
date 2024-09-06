{% set database_name = var('database_name', 'duckdb_stats') %}

-- workaround to be able to inject a variable and still use dbt_unit_testing
{% if execute and flags.WHICH in ['test', 'compile'] %}
    {# This block will only run during testing or compilation #}
    {% set source_table = dbt_unit_testing.source(
        'external_source' if var('data_source') == 'external_source' else 'duckdb_stats',
        'pypi_file_downloads'
    ) %}
{% else %}
    {# This block will run during normal model execution #}
    {% set source_table = source(
        'external_source' if var('data_source') == 'external_source' else database_name,
        'pypi_file_downloads'
    ) %}
{% endif %}

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
        {{ source_table }}
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
