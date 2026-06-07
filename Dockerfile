FROM python:3.13-slim

ARG TARGETARCH
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ADD https://github.com/amacneil/dbmate/releases/download/v2.26.0/dbmate-linux-${TARGETARCH} /usr/local/bin/dbmate
RUN chmod +x /usr/local/bin/dbmate

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY squawk/ squawk/
COPY libs/ libs/
COPY db/migrations/ db/migrations/
RUN uv sync --locked --no-dev

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH"
ENTRYPOINT ["./entrypoint.sh"]
CMD ["squawk"]
