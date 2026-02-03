# 📚 Boogle — Open Source Search Engine for Free Books

**Boogle** is a distinctively fast, open-source search engine designed to index and search public domain books from multiple sources.

It combines a **Python** orchestrator (FastAPI, SQL adapters) with a high-performance **Rust** indexing engine (BM25 ranking, compression) to deliver millisecond-level search latencies over large text corpora.

---

## Overview

Most public-domain book collections (like Project Gutenberg or Open Library) provide their own search features,
but none of them aggregate multiple sources or offer relevance ranking based on modern information retrieval techniques.

**Boogle** changes that.
It unifies data from different repositories, builds its own index,
and returns ranked results according to query relevance — just like a miniature, open-source version of Google Books.

---

## 🛠 Prerequisites

- **[Rust](https://www.rust-lang.org/tools/install)** (latest stable)
- **[uv](https://docs.astral.sh/uv/getting-started/installation/)** (fast Python package installer)
- **Docker** (optional, for PostgreSQL mode)

---

## ⚙️ Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/enrell/boogle.git
   cd boogle
   ```

2. **Install Python dependencies:**
   ```bash
   uv sync
   ```

3. **Build the Rust indexing extension:**
   ```bash
   uv run maturin develop -m rust_bm25/Cargo.toml --release
   ```

---

## 🚀 Quick Start

### Option 1: SQLite (Easiest)
Get started immediately without any external database services.

1. **Seed & Index Books:**
   This command downloads 1000 books from Gutenberg and builds the search index.
   ```bash
   uv run boogle index --limit 1000 --sqlite
   ```

2. **Search via CLI:**
   ```bash
   uv run boogle search "liberty and death" --sqlite
   ```

3. **Start the API Server:**
   ```bash
   uv run boogle api --sqlite
   ```
   > 📄 API Documentation available at: `http://127.0.0.1:8000/docs`

### Option 2: PostgreSQL (Production)
Recommended for larger datasets and better concurrency.

1. **Start the Database:**
   ```bash
   docker compose up -d db
   ```

2. **Run Migrations:**
   ```bash
   uv run boogle-db migrate
   ```

3. **Seed & Index Books:**
   ```bash
   uv run boogle index --limit 1000
   ```

4. **Start the API Server:**
   ```bash
   uv run boogle api
   ```

---

## 📖 CLI Reference

Boogle exposes two main CLI tools: `boogle` (APP) and `boogle-db` (DB Ops).

### `boogle` - Application Pipeline
| Command | Description | Flags |
|---------|-------------|-------|
| `index` | Downloads books and builds the BM25 index | `--limit N` `--sqlite` `--workers N` `--reindex` |
| `search` | Performs a search query via CLI | `query` `--top-k N` `--sqlite` |
| `api` | Starts the FastAPI server | `--port N` `--host 0.0.0.0` `--sqlite` |

### `boogle-db` - Database Management (Postgres)
| Command | Description |
|---------|-------------|
| `migrate` | Creates necessary tables (`books`, `seed_offsets`) |
| `clear-all`| Truncates all tables (Data Reset) |
| `test` | Verifies database connection and schema |

---

## 📊 Benchmarking

**Search Performance:**
Measure latency and QPS (Queries Per Second) for the ranking engine.
```bash
uv run scripts/benchmark.py --iterations 10
# Use --sqlite for SQLite mode
```

**Indexing Performance:**
Measure write throughput and indexing speed on a synthetic segment.
```bash
uv run bench_index.py
```

---

## 🪪 License

This project is open-source under the **MIT License**.
Feel free to fork, modify, and improve!

> *Boogle — Free Books. Free Knowledge.*
