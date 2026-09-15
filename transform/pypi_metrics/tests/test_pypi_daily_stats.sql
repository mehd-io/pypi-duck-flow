{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test ('pypi_daily_stats','counts_distribution_artifacts_only') %}

  {% call dbt_unit_testing.mock_source('external_source', 'pypi_file_downloads') %}
    SELECT
      '2023-04-02 14:49:15+02'::timestamp AS timestamp,
      'US' AS country_code,
      '/packages/38/5b/...' AS url,
      'duckdb' AS project,
      STRUCT_PACK(
          filename := fixture.filename,
          project := 'duckdb',
          version := '0.7.1',
          type := fixture.file_type
      ) AS file,
      STRUCT_PACK(
          installer := NULL,
          python := '3.8.2',
          implementation := NULL,
          distro := NULL,
          system := STRUCT_PACK(name := 'Linux', release := '4.15.0-66-generic'),
          cpu := 'x86_64',
          openssl_version := NULL,
          setuptools_version := NULL,
          rustc_version := NULL
      ) AS details,
      'TLSv1.2' AS tls_protocol,
      'ECDHE-RSA-AES128-GCM-SHA256' AS tls_cipher
    FROM (
      VALUES
        ('duckdb-0.7.1-cp38-cp38-manylinux.whl', 'bdist_wheel'),
        ('duckdb-0.7.1.tar.gz', 'sdist'),
        ('duckdb-0.7.1.zip', 'sdist'),
        ('duckdb-0.7.1-cp38-cp38-manylinux.whl.metadata', 'bdist_wheel'),
        ('duckdb-0.7.1.tar.gz.asc', 'sdist'),
        ('duckdb-0.7.1.egg', 'bdist_egg'),
        ('duckdb-0.7.1.exe', 'bdist_wininst'),
        (NULL, NULL)
    ) AS fixture(filename, file_type)
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
    SELECT
      '2023-04-02'::date AS download_date,
      'duckdb' AS project,
      '3.8' AS python_version,
      'x86_64' AS cpu,
      'Linux' AS system_name,
      3 AS daily_download_sum,
      8 AS legacy_daily_download_sum
  {% endcall %}

{% endcall %}
