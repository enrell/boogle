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

### Option 2: PostgreSQL (Local Development)
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

### Option 3: Docker Compose (Full Stack)
Run the complete stack in containers — ideal for deployment or testing without local dependencies.

1. **Build and Start All Services:**
   ```bash
   docker compose up -d
   ```
   This starts PostgreSQL, runs migrations, and launches the API server.

2. **Run Migrations (one-time):**
   ```bash
   docker compose run --rm migrate
   ```

3. **Index Books:**
   ```bash
   docker compose run --rm index uv run boogle index --limit 1000
   ```

4. **Test the API:**
   ```bash
   curl http://localhost:8000/health
   curl "http://localhost:8000/search?query=liberty&limit=5"
   ```

5. **View Logs:**
   ```bash
   docker compose logs -f api
   ```

6. **Stop Everything:**
   ```bash
   docker compose down        # Keep data
   docker compose down -v     # Remove data volumes
   ```

> **Note:** The API is available at `http://localhost:8000` and Adminer (DB UI) at `http://localhost:8080`.

---

## 📖 CLI Reference

Boogle exposes two main CLI tools: `boogle` (APP) and `boogle-db` (DB Ops).

### `boogle` - Application Pipeline
| Command | Description | Flags |
|---------|-------------|-------|
| `index` | Downloads books and builds the BM25 index | `--limit N` `--sqlite` `--workers N` `--reindex` `--light-mode` `--enrich` |
| `search` | Performs a search query via CLI | `query` `--top-k N` `--sqlite` `--light-mode` |
| `api` | Starts the FastAPI server | `--port N` `--host 0.0.0.0` `--sqlite` |

### `boogle-db` - Database Management (Postgres)
| Command | Description |
|---------|-------------|
| `migrate` | Creates necessary tables (`books`, `seed_offsets`) |
| `clear-all`| Truncates all tables (Data Reset) |
| `test` | Verifies database connection and schema |

---

## 📊 Benchmarking

Boogle includes a comprehensive benchmark suite to test indexing throughput, ranking latency, and API performance.

**1. Indexing Performance:**
Measures how fast books can be processed and indexed.
```bash
uv run scripts/benchmark.py indexing
```

**2. API Stress Test:**
Measures end-to-end latency and QPS against a running server.
```bash
uv run scripts/benchmark.py api --url http://127.0.0.1:8000 --concurrency 10
```

**3. Internal Library Benchmark:**
Micro-benchmarks the Rust ranking engine + DB lookups directly.
```bash
uv run scripts/benchmark.py library --sqlite
```

**Run All:**
```bash
uv run scripts/benchmark.py all --sqlite
```

---

## ⚡ Realtime Indexing

Boogle supports **realtime indexing** for adding documents on the fly without rebuilding the entire index.

### Architecture
The realtime indexer uses a hybrid LSM-tree design:
- **Disk segments**: Immutable BM25 index files (from batch indexing)
- **RAM buffer**: In-memory index for newly added documents
- **WAL (Write-Ahead Log)**: Durability for in-flight documents

Search queries are federated across both disk and memory, with results merged and ranked.

### Enable Realtime Mode
Set the `REALTIME_INDEX` environment variable:
```bash
REALTIME_INDEX=1 uv run boogle api --sqlite
```

### API Endpoints

**Add a document:**
```bash
curl -X POST http://localhost:8000/documents \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Full text content of the book...",
    "book_id": "custom-123",
    "title": "My Custom Book",
    "author": "John Doe"
  }'
```

**Flush memory buffer:**
```bash
curl -X POST http://localhost:8000/documents/flush
```

**Check mode:**
```bash
curl http://localhost:8000/health
# Returns: {"status": "healthy", "mode": "realtime"}
```

### Notes
- Documents added via `/documents` are immediately searchable
- The WAL ensures documents survive server restarts
- Call `/documents/flush` after persisting documents to disk via batch indexing

---

## 💡 Light Mode

Boogle supports **Light Mode** for scenarios where you want to index and search book metadata (title, author, subjects, language) without downloading full text content. This reduces storage by ~100x and enables rapid indexing of large catalogs.

### When to Use Light Mode

- **Discovery/Browsing**: When you want to find books by title, author, or subject
- **Large Catalogs**: Index tens of thousands of books quickly without storing GBs of text
- **Limited Storage**: Run on resource-constrained environments
- **Metadata Research**: Analyze book metadata without content

### Comparison

| Feature | Full Mode | Light Mode |
|---------|-----------|------------|
| Storage per book | ~1-5 MB | ~10 KB |
| Index time (1000 books) | ~30 min | ~2 min |
| Searchable content | Full text | Metadata only |
| Search types | Any text | Title, Author, Subjects |
| Snippets | Yes | No |
| Use case | Deep reading | Discovery, browsing |

### Usage

**CLI - Index in Light Mode:**
```bash
uv run boogle index --light-mode --limit 1000 --sqlite
```

**CLI - Search in Light Mode:**
```bash
uv run boogle search "shakespeare tragedy" --light-mode --sqlite
```

**API Server - Light Mode:**
```bash
LIGHT_MODE=1 uv run boogle api --sqlite
```

The `/search` endpoint will automatically use the metadata-only index.

**Check mode:**
```bash
curl http://localhost:8000/health
# Returns: {"status": "healthy", "mode": "light"}
```

### How It Works

1. **Metadata-only seeding**: Downloads only book metadata from Gutenberg catalog (no file downloads)
2. **Metadata indexing**: Creates a lightweight BM25 index on title, author, subjects, and language
3. **BM25 scoring**: Uses standard BM25 ranking with term frequency weighting
4. **Field boosting**: Title matches are boosted 3x, subjects 2x for relevance

### Storage Layout

- **Light mode index**: `data/index_metadata/`
- **Light mode checkpoints**: `data/books/.checkpoint_light`
- **Full mode index**: `data/index/`
- **Full mode checkpoints**: `data/books/.checkpoint`

### Migration Between Modes

You can run both modes on the same database:

```bash
# First, seed metadata in light mode
uv run boogle index --light-mode --sqlite

# Later, download full text for specific books
# (Re-run without --light-mode for books you want to read)
```

**Note**: Light mode and full mode use separate checkpoints and indexes. They do not interfere with each other.

---

## 🔌 Book Providers

Boogle supports multiple book providers through a pluggable architecture. Adding a new provider is as simple as creating a single Python file.

### Available Providers

| Provider | Description | Downloads | Default |
|----------|-------------|-----------|---------|
| **gutenberg** | Project Gutenberg | ✅ Yes | ✅ Enabled |
| **openlibrary** | Open Library metadata | ❌ No | ❌ Disabled |
| **bndigital** | Brazilian National Library | ❌ No | ❌ Disabled |

### Enabling Providers

Providers can be enabled via environment variables:

```bash
# Enable Open Library
export BOOGLE_PROVIDER_OPENLIBRARY_ENABLED=1

# Enable BNDigital
export BOOGLE_PROVIDER_BNDIGITAL_ENABLED=1

# Disable Gutenberg (if you only want other providers)
export BOOGLE_PROVIDER_GUTENBERG_ENABLED=0

# Run with specific providers
BOOGLE_PROVIDER_OPENLIBRARY_ENABLED=1 uv run boogle index --sqlite
```

### Using Providers

**Index from specific providers:**

```bash
# Index only from Open Library
uv run boogle index --sqlite --providers openlibrary

# Index from multiple providers
uv run boogle index --sqlite --providers gutenberg,openlibrary

# Index from all enabled providers (default)
uv run boogle index --sqlite
```

**Search with provider filter:**

```bash
# Search across all providers (default)
uv run boogle search "shakespeare" --sqlite

# Search only in specific provider
uv run boogle search "machado de assis" --sqlite --source bndigital
```

### Creating a New Provider

Adding a new provider takes just 3 steps:

1. **Create a provider file** (`src/providers/myprovider.py`):

```python
from src.providers.base import BaseBookProvider
from src.providers.registry import register_provider

@register_provider
class MyProvider(BaseBookProvider):
    @property
    def source_name(self) -> str:
        return "myprovider"
    
    def iter_book_metadata(self, limit=None):
        # Yield book metadata dicts
        for book in my_book_source:
            yield {
                'source': self.source_name,
                'book_id': str(book['id']),
                'title': book['title'],
                'author': book['author'],
                'url': self.get_book_url(book['id']),
            }
    
    def extract_metadata(self, book_id: str):
        # Fetch single book metadata
        book = fetch_book(book_id)
        return {
            'source': self.source_name,
            'book_id': book_id,
            'title': book['title'],
            'author': book['author'],
            'url': self.get_book_url(book_id),
        }
    
    def get_book_url(self, book_id: str) -> str:
        return f"https://mysite.com/book/{book_id}"
```

2. **Enable your provider**:

```bash
export BOOGLE_PROVIDER_MYPROVIDER_ENABLED=1
```

3. **Use it**:

```bash
uv run boogle index --providers myprovider --sqlite
```

That's it! Your provider is automatically discovered and integrated with all Boogle phases (seeding, indexing, search, API).

### Provider Requirements

**Minimal implementation** (3 required methods):
- `source_name` - Unique provider identifier
- `iter_book_metadata()` - Stream all books
- `extract_metadata()` - Fetch single book

**Optional features**:
- `download_book()` - Full text downloads
- `search_books()` - Provider-specific search
- `filter_book()` - Custom filtering logic
- `get_cover_url()` - Cover images

See `src/providers/example.py` for a complete template with documentation.

### Testing Providers

Run the provider test suite:

```bash
# Test all providers
python test_providers.py

# Test specific provider
python test_providers.py --test-openlibrary

# Skip network tests
python test_providers.py --skip-network
```

---

## 🪪 License

This project is open-source under the **MIT License**.
Feel free to fork, modify, and improve!

> *Boogle — Free Books. Free Knowledge.*
