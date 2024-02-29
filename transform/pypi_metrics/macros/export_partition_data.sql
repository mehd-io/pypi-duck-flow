{% macro export_partition_data(date_column, table) %}
{% set s3_path = env_var('TRANSFORM_S3_PATH_OUTPUT', 'my-bucket-path') %}
    COPY (
        SELECT *,
            YEAR({{ date_column }}) AS year, 
            MONTH({{ date_column }}) AS month 
        FROM {{ table }}
    ) 
    TO '{{ s3_path }}/{{ table }}'
     (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
{% endmacro %}
