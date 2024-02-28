{% macro export_partition_data(date_column, table) %}
{% set s3_path = env_var('S3_PATH_OUTPUT_BUCKET', 'my-bucket-path') %}
    COPY (
        SELECT *,
            YEAR({{ date_column }}) AS year, 
            MONTH({{ date_column }}) AS month 
        FROM {{ table }}
    ) 
    TO '{{ s3_path }}/{{ table }}'
     (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
{% endmacro %}
