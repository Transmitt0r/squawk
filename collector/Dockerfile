FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy workspace manifests for dependency resolution (cache layer)
COPY pyproject.toml uv.lock .python-version ./
COPY collector/pyproject.toml collector/pyproject.toml
COPY bot/pyproject.toml bot/pyproject.toml
RUN uv sync --locked --no-dev --no-install-project --package flighttracker

# Copy source and install
COPY collector/collector/ collector/collector/
RUN uv sync --locked --no-dev --package flighttracker

ENV PATH="/app/.venv/bin:$PATH"
CMD ["squawk-collector"]
