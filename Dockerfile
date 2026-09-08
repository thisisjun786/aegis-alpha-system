# Standalone AAS CLI. User data lives in AAS_HOME, never in this image.
FROM ghcr.io/astral-sh/uv:0.11.32@sha256:df4cae8f3a96d175e2e5f992e597550000edbe78fdc2594d5cd8de1a217f504c AS uv
FROM python:3.13.13-slim-bookworm@sha256:355bfa66770995d7e9a0da4b3473b44d0cb451f6b56f5615ad9c39e3c4eca03f AS builder
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock README.md LICENSE THIRD_PARTY_NOTICES.md ./
RUN UV_PROJECT_ENVIRONMENT=/opt/venv uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN UV_PROJECT_ENVIRONMENT=/opt/venv uv sync --locked --no-dev --no-editable

FROM python:3.13.13-slim-bookworm@sha256:355bfa66770995d7e9a0da4b3473b44d0cb451f6b56f5615ad9c39e3c4eca03f AS runtime
RUN groupadd --gid 1000 aas && useradd --uid 1000 --gid aas --create-home --shell /usr/sbin/nologin aas
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY LICENSE THIRD_PARTY_NOTICES.md ./
COPY examples ./examples
ENV PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AAS_HOME=/state/aas
USER aas
ENTRYPOINT ["aas"]
CMD ["status"]
