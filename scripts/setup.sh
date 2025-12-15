#!/bin/bash
set -e
cd "$(dirname "$0")/.."
uv sync
uv run maturin develop --release --manifest-path rust_bm25/Cargo.toml
echo "Setup complete"
