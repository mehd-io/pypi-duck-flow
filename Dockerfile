# This connect the image to the repository 
LABEL org.opencontainers.image.source https://github.com/mehd-io/pypi-duck-flow

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
# Install only runtime dependencies
RUN poetry install --no-dev --no-interaction --no-ansi
# Copy the codebase
COPY ./ingestion ./ingestion
COPY ./transform ./transform

# Default command to keep container running for interactive `make` commands
CMD ["sleep", "infinity"]
