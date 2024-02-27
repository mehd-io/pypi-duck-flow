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
    END AS python_version,
    COUNT(*) AS daily_download_sum
FROM
    {{ source('external_source', 'pypi_file_downloads') }}
WHERE
    download_date >= '{{ var("start_date") }}'
    AND download_date < '{{ var("end_date") }}'
GROUP BY
    ALL
