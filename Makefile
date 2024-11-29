-include .env
export

REPOSITORY=mehd-io/pypi-duck-flow
DBT_FOLDER = transform/pypi_metrics/
DBT_TARGET = dev
DBT_DATA_SOURCE = motherduck
DATABASE_NAME ?= duckdb_stats
DOCKER ?= false
DOCKER_CMD = 
DOCKER_IMAGE ?= ghcr.io/$(REPOSITORY)
TRANSFORM_S3_PATH_INPUT ?= s3://whatever

ifeq ($(DOCKER),true)
    DOCKER_CMD = docker run --rm -w /app \
        -v $(GOOGLE_APPLICATION_CREDENTIALS):$(GOOGLE_APPLICATION_CREDENTIALS) \
        -e GOOGLE_APPLICATION_CREDENTIALS=$(GOOGLE_APPLICATION_CREDENTIALS) \
        -e motherduck_token=$(motherduck_token) \
        $(DOCKER_IMAGE)
endif

.PHONY : help pypi-ingest format test aws-sso-creds pypi-transform 

pypi-ingest: 
	$(DOCKER_CMD) uv run python3 -m ingestion.pipeline \
		--start_date $$START_DATE \
		--end_date $$END_DATE \
		--pypi_project $$PYPI_PROJECT \
		--database_name $$DATABASE_NAME \
		--s3_path $$S3_PATH \
		--aws_profile $$AWS_PROFILE \
		--gcp_project $$GCP_PROJECT \
		--timestamp_column $$TIMESTAMP_COLUMN \
		--destination $$DESTINATION

pypi-ingest-test:
	uv run pytest ingestion/tests

pypi-transform:
	$(DOCKER_CMD) uv run dbt run \
		--target $$DBT_TARGET \
		--project-dir $$DBT_FOLDER \
		--profiles-dir $$DBT_FOLDER \
		--vars '{"start_date": "$(START_DATE)", "end_date": "$(END_DATE)", "data_source": "$(DBT_DATA_SOURCE)", "database_name": "$(DATABASE_NAME)"}'

# Note : start_date and end_date depends on the mock data in the test
pypi-transform-test:
	$(DOCKER_CMD) uv run dbt test \
		--target dev \
		--project-dir $$DBT_FOLDER \
		--profiles-dir $$DBT_FOLDER \
		--vars '{"start_date": "2023-04-01", "end_date": "2023-04-03", "data_source": "external_source"}' 

## Docker 
build:
	docker build --label org.opencontainers.image.source=https://github.com/$(GITHUB_REPOSITORY) -t $(DOCKER_IMAGE) --build-arg PLATFORM=arm64 .

## Development
install: 
	uv sync --dev

format:
	uv run ruff format . 

dbt-deps:
	$(DOCKER_CMD) uv run dbt deps \
		--project-dir $$DBT_FOLDER \
		--profiles-dir $$DBT_FOLDER 

aws-sso-creds:
# DuckDB aws creds doesn't support loading from sso, so this create temporary creds file
	aws configure export-credentials --profile $$AWS_PROFILE --format env-no-export | \
	grep -E 'AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN' | \
	sed -e 's/AWS_ACCESS_KEY_ID/aws_access_key_id/' \
		-e 's/AWS_SECRET_ACCESS_KEY/aws_secret_access_key/' \
		-e 's/AWS_SESSION_TOKEN/aws_session_token/' \
		-e 's/^/ /' -e 's/=/ =/' | \
	awk -v profile="$$AWS_PROFILE" 'BEGIN {print "["profile"]"} {print}' > ~/.aws/credentials
