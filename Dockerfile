FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.1 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Dependencies first so code edits do not invalidate this layer.
COPY pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --frozen --no-dev --no-install-project --extra snowflake

COPY src ./src
COPY snowflake ./snowflake
COPY debezium ./debezium
COPY kafka ./kafka
COPY sample_events ./sample_events
RUN uv sync --frozen --no-dev --extra snowflake

RUN useradd --create-home --uid 10001 app && mkdir -p /data && chown app /data
USER app

CMD ["cdc-consumer"]
