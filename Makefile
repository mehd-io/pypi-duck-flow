-include .env
export

REPOSITORY=mehd-io/pypi-duck-flow
DBT_FOLDER = transform/pypi_metrics/
DBT_TARGET = dev
DOCKER ?= false
DOCKER_CMD = 
DOCKER_IMAGE ?= ghcr.io/$(REPOSITORY)

ifeq ($(DOCKER),true)
    DOCKER_CMD = docker run --rm -v $(PWD):/app -w /app \
        -v $(GOOGLE_APPLICATION_CREDENTIALS):$(GOOGLE_APPLICATION_CREDENTIALS) \
        -e GOOGLE_APPLICATION_CREDENTIALS=$(GOOGLE_APPLICATION_CREDENTIALS) \
        $(DOCKER_IMAGE)
endif

.PHONY : help pypi-ingest format test aws-sso-creds pypi-transform 

pypi-ingest: 
	$(DOCKER_CMD) poetry run python3 -m ingestion.pipeline \
		--start_date $$START_DATE \
		--end_date $$END_DATE \
		--pypi_project $$PYPI_PROJECT \
		--table_name $$TABLE_NAME \
		--s3_path $$S3_PATH \
		--aws_profile $$AWS_PROFILE \
		--gcp_project $$GCP_PROJECT \
		--timestamp_column $$TIMESTAMP_COLUMN \
		--destination $$DESTINATION

pypi-ingest-test:
	pytest ingestion/tests

pypi-transform:
	$(DOCKER_CMD) cd $$DBT_FOLDER && \
	dbt run \
		--target $$DBT_TARGET \
		--vars '{"start_date": "$(START_DATE)", "end_date": "$(END_DATE)"}'

# Note : start_date and end_date depends on the mock data in the test
pypi-transform-test:
	cd $$DBT_FOLDER && \
	dbt test \
		--vars '{"start_date": "2023-04-01", "end_date": "2023-04-03"}'

## Docker 
build:
	docker build --label org.opencontainers.image.source=https://github.com/$(GITHUB_REPOSITORY) -t $(DOCKER_IMAGE) --build-arg PLATFORM=arm64 .

## Development
install: 
	poetry install

format:
	ruff format . 


aws-sso-creds:
# DuckDB aws creds doesn't support loading from sso, so this create temporary creds file
	aws configure export-credentials --profile $$AWS_PROFILE --format env-no-export | \
	grep -E 'AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN' | \
	sed -e 's/AWS_ACCESS_KEY_ID/aws_access_key_id/' \
		-e 's/AWS_SECRET_ACCESS_KEY/aws_secret_access_key/' \
		-e 's/AWS_SESSION_TOKEN/aws_session_token/' \
		-e 's/^/ /' -e 's/=/ =/' | \
	awk -v profile="$$AWS_PROFILE" 'BEGIN {print "["profile"]"} {print}' > ~/.aws/credentials
