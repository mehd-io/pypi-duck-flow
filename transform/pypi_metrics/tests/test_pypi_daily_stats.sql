{{ config(tags=['unit-test']) }}

{% call dbt_unit_testing.test ('pypi_daily_stats','check_duckdb_downloads_on_20230402') %}
  
  {% call dbt_unit_testing.mock_source('external_source', 'pypi_file_downloads') %}
    SELECT 
      '2023-04-02 14:49:15+02'::timestamp AS timestamp, 
      'US' AS country_code, 
      '/packages/38/5b/...' AS url, 
      'duckdb' AS project, 
      NULL AS file, -- Assuming the 'file' struct is not essential for this test
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
    UNION ALL
    SELECT 
      '2023-04-02 14:49:15+02'::timestamp AS timestamp, 
      'US' AS country_code, 
      '/packages/38/5b/...' AS url, 
      'duckdb' AS project, 
      NULL AS file, -- Assuming the 'file' struct is not essential for this test
      STRUCT_PACK(
          installer := NULL,
          python := '3.8.1',
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
    -- Add more rows as needed for your test
  {% endcall %}

{% call dbt_unit_testing.expect() %}
    SELECT 
      '2023-04-02'::date AS download_date, 
      'duckdb' AS project,
      '3.8' AS python_version,
      'x86_64' AS cpu,
      'Linux' AS system_name,
      2 AS daily_download_sum -- Adjust this based on the expected outcome of your test
  {% endcall %}

{% endcall %}
