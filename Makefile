include .env
export

DBT_FOLDER = transform/pypi_metrics/
DBT_TARGET = dev

.PHONY : help pypi-ingest format test aws-sso-creds pypi-transform

pypi-ingest: 
	poetry run python3 -m ingestion.pipeline \
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
	cd $$DBT_FOLDER && \
	dbt run \
		--target $$DBT_TARGET \
		--vars '{"start_date": "$(START_DATE)", "end_date": "$(END_DATE)"}'

# Note : start_date and end_date depends on the mock data in the test
pypi-transform-test:
	cd $$DBT_FOLDER && \
	dbt test \
		--vars '{"start_date": "2023-04-01", "end_date": "2023-04-03"}'

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
