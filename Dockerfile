# Stage 1: Base
FROM python:3.11 as base

# Install Poetry
RUN pip install poetry --no-cache-dir

# Stage : Development
FROM base as development
# Set working directory
WORKDIR /app
# Copy Poetry configuration files
COPY pyproject.toml poetry.lock ./
# Install development dependencies
RUN poetry config virtualenvs.create false && poetry install

# Stage : Production
FROM base as production
# Set working directory
WORKDIR /app
# Copy Poetry configuration files
COPY Makefile pyproject.toml poetry.lock ./
# Copy the codebase
COPY ./ingestion ./ingestion
COPY ./transform ./transform
# Install only runtime dependencies
RUN poetry config virtualenvs.create false && poetry install --no-dev --no-interaction --no-ansi
RUN make dbt-deps

# Default command to keep container running for interactive `make` commands
CMD ["sleep", "infinity"]
