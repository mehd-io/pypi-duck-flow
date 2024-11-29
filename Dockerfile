# Stage 1: Base
FROM python:3.12 as base

# Install UV via pip
RUN pip install uv==0.5.5 --no-cache-dir

# UV environment settings
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/local/bin/python3.12

# Stage : Development
FROM base as development
# Set working directory
WORKDIR /app

# Copy UV configuration files
COPY pyproject.toml uv.lock ./

# Install development dependencies
RUN --mount=type=cache,target=/root/.cache \
    uv sync --locked

# Stage : Production
FROM base as production
# Set working directory
WORKDIR /app

# Copy UV configuration files and codebase
COPY Makefile pyproject.toml uv.lock ./
COPY ./ingestion ./ingestion
COPY ./transform ./transform

# Install only runtime dependencies
RUN --mount=type=cache,target=/root/.cache \
    uv sync --locked --no-dev --no-install-project

# Run additional setup (e.g., dbt dependencies)
RUN make dbt-deps

# Default command to keep container running for interactive `make` commands
CMD ["sleep", "infinity"]
