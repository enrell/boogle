# 📚 Boogle — Open Source Search Engine for Free Books

**Boogle** is an open-source search engine designed to index and search books from multiple free and public-domain sources.  
The goal is to make it easy for readers, students, and researchers to find *free and legal* books across the web —  
without having to visit each site individually.

---

## Overview

Most public-domain book collections (like Project Gutenberg or Open Library) provide their own search features,
but none of them aggregate multiple sources or offer relevance ranking based on modern information retrieval techniques.

**Boogle** changes that.
It unifies data from different repositories, builds its own index,
and returns ranked results according to query relevance — just like a miniature, open-source version of Google Books.

---

## ⚙️ Installation

### Prerequisites

- **[Rust](https://www.rust-lang.org/tools/install)** (latest stable)
- **[uv](https://docs.astral.sh/uv/getting-started/installation/)** (fast Python package installer)

### Setup

1. **Install dependencies:**
   ```bash
   uv sync
   ```

2. **Build the Rust extension:**
   ```bash
   uv run maturin develop -m rust_bm25/Cargo.toml --release
   ```

---

## 🚀 Quick Start

Boogle supports two database backends:
- **PostgreSQL** (recommended for production)
- **SQLite** (perfect for demos, development, or environments without PostgreSQL)

### Option 1: PostgreSQL Setup (Recommended)

1. **Start the database:**
   ```bash
   docker compose up -d db
   ```

2. **Download books from Project Gutenberg:**
   ```bash
   uv run boogle seed --limit 1000
   ```

3. **Build the search index:**
   ```bash
   uv run boogle index
   ```

4. **Start the API server:**
   ```bash
   uv run boogle api
   ```

5. **Try it out:**
   - API Docs: `http://127.0.0.1:8000/docs`
   - Search: `uv run boogle search "love and war"`

### Option 2: SQLite Setup (No Docker Required!)

Perfect for trying out Boogle without PostgreSQL:

1. **Download books from Project Gutenberg:**
   ```bash
   uv run boogle seed --limit 1000 --sqlite
   ```

2. **Update metadata (important for SQLite!):**
   ```bash
   uv run boogle update-metadata --sqlite
   ```

3. **Build the search index:**
   ```bash
   uv run boogle index --full --sqlite
   ```

4. **Start the API server:**
   ```bash
   uv run boogle api --sqlite
   ```

5. **Try it out:**
   - API Docs: `http://127.0.0.1:8000/docs`
   - Search: `uv run boogle search "independence" --sqlite`

### CLI Commands Reference

| Command | Description | SQLite Flag |
|---------|-------------|-------------|
| `boogle seed --limit N` | Download N books from Gutenberg | `--sqlite` |
| `boogle update-metadata` | Update metadata for downloaded books | `--sqlite` |
| `boogle index` | Build/update search index | `--sqlite` |
| `boogle index --full` | Full reindex (clears existing data) | `--sqlite` |
| `boogle search "query"` | Search from CLI | `--sqlite` |
| `boogle api` | Start the FastAPI server | `--sqlite` |

**Note:** The SQLite database is stored at `data/boogle.db` by default.

---

## 📊 Benchmarking

You can run a performance benchmark to measure query latency, throughput (QPS), and index statistics:

```bash
uv run scripts/benchmark.py
```

## Benchmarking with SQLite

```bash
uv run scripts/benchmark.py --sqlite
```

Options:
- `--iterations N`: Number of iterations per query (default: 5)
- `--warmup N`: Number of warmup runs (default: 2)

---

## 🌐 Data Sources

- [x] [Project Gutenberg](https://www.gutenberg.org/)
- [ ] [Open Library](https://openlibrary.org/)
- [ ] [Wikisource](https://wikisource.org/)
- [ ] [Public Domain Library](https://publicdomainlibrary.org/)
- [ ] [Internet Archive](https://archive.org/details/texts)
- [ ] [Domínio Público (Brazil)](http://www.dominiopublico.gov.br/)

Contact: **[enrellsa10@proton.me](mailto:enrellsa10@proton.me)**

---

## 🪪 License

This project is open-source under the **MIT License**.
Feel free to fork, modify, and improve!

---

> *Boogle — Free Books. Free Knowledge.*
