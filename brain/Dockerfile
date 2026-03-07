FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY core/ core/
COPY brain/ brain/

ENV PYTHONPATH=/app
CMD ["uv", "run", "python", "-m", "brain.main"]
