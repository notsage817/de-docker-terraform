# Use a lightweight Python image
FROM python:3.13-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy dependency definitions
COPY pyproject.toml uv.lock ./

# Install dependencies into the system python (no virtualenv needed inside Docker)
RUN uv sync --frozen
ENV PATH="/app/.venv/bin:$PATH"
# Copy the script
COPY ingest.py .

# default entrypoint
ENTRYPOINT ["python", "ingest.py"]