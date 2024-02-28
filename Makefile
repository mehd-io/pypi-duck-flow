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

pypi-transform:
	dbt run \
		--target $$DBT_TARGET \
		--project-dir $$DBT_FOLDER \
		--vars '{"start_date": "$$START_DATE", "end_date": "$$END_DATE"}'

## Development
install: 
	poetry install

format:
	ruff format . 

test:
	pytest tests

aws-sso-creds:
# DuckDB aws creds doesn't support loading from sso, so this create temporary creds file
	aws configure export-credentials --profile $$AWS_PROFILE --format env-no-export | \
	grep -E 'AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN' | \
	sed -e 's/AWS_ACCESS_KEY_ID/aws_access_key_id/' \
		-e 's/AWS_SECRET_ACCESS_KEY/aws_secret_access_key/' \
		-e 's/AWS_SESSION_TOKEN/aws_session_token/' \
		-e 's/^/ /' -e 's/=/ =/' | \
	awk -v profile="$$AWS_PROFILE" 'BEGIN {print "["profile"]"} {print}' > ~/.aws/credentials
