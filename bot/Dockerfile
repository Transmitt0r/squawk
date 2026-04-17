FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-humor-sans \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy workspace manifests for dependency resolution (cache layer)
COPY pyproject.toml uv.lock .python-version ./
COPY collector/pyproject.toml collector/pyproject.toml
COPY bot/pyproject.toml bot/pyproject.toml
RUN uv sync --locked --no-dev --no-install-project --package squawk-bot

# Copy source and install
COPY bot/bot/ bot/bot/
RUN uv sync --locked --no-dev --package squawk-bot

ENV PATH="/app/.venv/bin:$PATH"
CMD ["squawk-bot"]
