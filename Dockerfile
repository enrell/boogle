FROM ghcr.io/astral-sh/uv:python3.13-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential curl && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    rm -rf /var/lib/apt/lists/*
ENV PATH="/root/.cargo/bin:${PATH}"

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY rust_bm25 ./rust_bm25
COPY src ./src
COPY scripts ./scripts
COPY app.py main.py README.md ./

RUN uv run maturin develop --release -m rust_bm25/Cargo.toml

CMD ["uv", "run", "uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
