FROM python:3.13-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-humor-sans \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY bot/ bot/
RUN uv sync --locked --no-dev \
    && python -c "import matplotlib.font_manager; matplotlib.font_manager._load_fontmanager(try_read_cache=False)"

ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "-m", "bot"]
