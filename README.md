# Pypi Duck Flow : Get insights of your python project 🐍

This project is a collections of pipelines to get insights of your python project. It also serves as educational purpose (YouTube videos and blogs) to learn how to build data pipelines with **Python**, **SQL** & **DuckDB**.

The project is composed of series in 3 parts :
- Ingestion ([YouTube video](https://youtu.be/3pLKTmdWDXk?si=ZI9fjoGQ7hHzznOZ))
- transformation (WIP)
- visualization (TODO)

## High level architecture
![High level architecture](./docs/etl_architecture.png)


## Development

### Setup

The project requires :
* Python 3.11
* Poetry for dependency management.

There's also a [devcontainer](https://code.visualstudio.com/docs/devcontainers/containers) for VSCode.
Finally a `Makefile` is available to run common tasks.

### Env & credentials

A `.env` file is required to run the project. You can copy the `.env.example` file and fill the required values.
```
TABLE_NAME=pypi_file_downloads # output table name
S3_PATH=s3://my-s3-bucket # output s3 path
AWS_PROFILE=default # aws profile to use
GCP_PROJECT=my-gcp-project # GCP project to use
START_DATE=2023-04-01 # start date of the data to ingest
END_DATE=2023-04-03 # end date of the data to ingest
PYPI_PROJECT=duckdb # pypi project to ingest
GOOGLE_APPLICATION_CREDENTIALS=/path/to/my/creds # path to GCP credentials
motherduck_token=123123 # MotherDuck token
TIMESTAMP_COLUMN=timestamp # timestamp column name, use for partitions on S#
DESTINATION=local,s3,md # destinations to push data to, can be one or more
```

## Ingestion

### Requirements

- [GCP account](https://console.cloud.google.com/)
- AWS S3 bucket (optional to push data to S3) and AWS credentials (at the default `~/.aws/credentials` path) that has write access to the bucket
- [MotherDuck account](https://app.motherduck.com/) (optional to push data to MotherDuck)

### Run
Once you fill your `.env` file, do the following :
* `make install` : to install the dependencies
* `make pypi-ingest` : to run the ingestion pipeline
* `make pypi-transform START_DATE=2023-04-05 END_DATE=2023-04-07 DBT_TARGET=dev` : example of a run using external table, change to prod for motherduck
