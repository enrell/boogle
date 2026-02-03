# Stage 1: Builder
# We use the full image to have access to build tools if needed, 
# and manually install Rust which is required for our extension.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm AS builder

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

# Install system build dependencies and Rust
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    ca-certificates \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && rm -rf /var/lib/apt/lists/*

# Add cargo to path
ENV PATH="/root/.cargo/bin:${PATH}"

# Install Python dependencies first to leverage caching
# README.md is required for pyproject.toml validation
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev

# Copy source code
COPY rust_bm25 ./rust_bm25
COPY src ./src
COPY scripts ./scripts
COPY app.py main.py ./

# Build the Rust extension into the virtual environment
RUN uv run maturin develop --release -m rust_bm25/Cargo.toml

# Stage 2: Runner
# Use a slim image for production to reduce size
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS runner

WORKDIR /app

# Copy the virtual environment from the builder
COPY --from=builder /app/.venv /app/.venv
# Ensure the venv executables are first in PATH
ENV PATH="/app/.venv/bin:$PATH"

# Copy application code
COPY src ./src
COPY scripts ./scripts
COPY app.py main.py README.md ./

# Run the API
# We use 'uv run' but since PATH hits the venv first, 
# 'python -m uvicorn' would also work. Keeping 'uv run' for consistency.
CMD ["uv", "run", "uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
