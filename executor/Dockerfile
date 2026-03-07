FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY core/ core/
COPY executor/ executor/

ENV PYTHONPATH=/app
CMD ["uv", "run", "python", "-m", "executor.main"]
