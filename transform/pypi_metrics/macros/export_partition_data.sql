{% macro export_partition_data(date_column, s3_path, table) %}
    COPY (
        SELECT *,
            YEAR({{ date_column }}) AS year, 
            MONTH({{ date_column }}) AS month 
        FROM {{ table }}
    ) 
    TO '{{ s3_path }}/{{ table }}'
     (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
{% endmacro %}
