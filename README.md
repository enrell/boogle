# 📚 Boogle — Open Source Search Engine for Free Books

[![Python](https://img.shields.io/badge/Python-3.13+-blue.svg)](https://www.python.org/)
[![Rust](https://img.shields.io/badge/Rust-1.80+-orange.svg)](https://www.rust-lang.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-green.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Boogle** is a distinctively fast, open-source search engine designed to index and search public domain books from multiple sources.

It combines a **Python** orchestrator (FastAPI, SQL adapters) with a high-performance **Rust** indexing engine (BM25 ranking, compression) to deliver millisecond-level search latencies over large text corpora.

## ✨ Highlights

- 🎯 **Multi-Provider**: Aggregate from Gutenberg, OpenLibrary, PPORTAL, and more
- 🔗 **Unified Schema**: Automatic cross-reference merging with quality scoring
- ⚡ **Parallel Indexing**: Seed from multiple providers simultaneously
- 🔒 **Security First**: SQL injection, XSS, and path traversal protection
- 🚀 **High Performance**: Rust-based BM25 with millisecond latency
- 💾 **Flexible Storage**: SQLite or PostgreSQL support
- 🌍 **Multilingual**: Automatic language detection (75+ languages)
- ✏️ **Smart Search**: Spell correction + query expansion with synonyms
- 📖 **Semantic Chunking**: Content-aware text splitting (chapter/paragraph/sentence)
- 🧹 **Space Efficient**: Streaming cleanup deletes source files automatically

---

## Overview

Most public-domain book collections (like Project Gutenberg or Open Library) provide their own search features,
but none of them aggregate multiple sources with quality-based ranking or offer modern information retrieval techniques.

**Boogle** changes that by providing:

🎯 **Multi-Provider Aggregation**: Automatically discovers and indexes books from multiple sources (Gutenberg, OpenLibrary, PPORTAL, and more)

🔗 **Unified Schema**: Cross-reference merging detects duplicates, combines metadata, and selects the best quality source

⚡ **Parallel Processing**: Seed from multiple providers simultaneously with thread-safe database connections

🔒 **Security First**: Comprehensive input validation, SQL injection blocking, XSS prevention, and rate limiting

🚀 **High Performance**: Rust-based BM25 index with millisecond search latency

💾 **Flexible Storage**: SQLite for quick start, PostgreSQL for production scale

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
    This command downloads 1000 books from all providers and builds the search index.
    ```bash
    uv run boogle index --limit 1000 --sqlite

    ```

2. **Search via CLI:**
    ```bash
    uv run boogle search "liberty and death" --sqlite
    ```

3. **Start the API Server:**
    ```bash
    # Light mode (metadata only, no full-text index needed)
    USE_SQLITE=1 LIGHT_MODE=1 uv run uvicorn src.api.main:app --port 8000

    # Full-text mode
    USE_SQLITE=1 uv run uvicorn src.api.main:app --port 8000
    ```
    > 📄 API Documentation available at: `http://127.0.0.1:8000/docs`

---

## 🚀 Advanced Features

### Enhanced Search API
The `/search` endpoint automatically applies NLP enhancements:

**1. Automatic Language Detection (75+ languages)**
```bash
# Portuguese query auto-detected
curl "http://localhost:8000/search?query=memórias+póstumas"
# Response includes: "language_detected": "pt"
```

**2. Spell Correction (SymSpell - 1000x faster)**
```bash
# Typo automatically corrected: shakspeare → shakespeare
curl "http://localhost:8000/search?query=shakspeare"
# Response includes: "corrected_query": "shakespeare"
```

**3. Query Expansion (WordNet synonyms)**
```bash
# "author" expands to: writer, poet, novelist, playwright
curl "http://localhost:8000/search?query=author"
# Response includes expansions in metadata
```

**4. Semantic Chunking**
Content-aware chunking preserves natural boundaries (chapter → paragraph → sentence):
```python
from rust_bm25 import chunk_text_semantic

# Automatically respects chapter/paragraph boundaries
chunks = chunk_text_semantic(text, target_size=500, overlap=50)
```

**5. Streaming Cleanup (Space Efficient)**
By default, Boogle deletes source files immediately after indexing:
```bash
# Saves 90%+ disk space (default behavior)
uv run boogle index --sqlite --limit 10000

# Keep source files for debugging
uv run boogle index --sqlite --limit 10000 --keep-books
```

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
| `index` | Downloads books and builds the BM25 index | `--limit N` `--sqlite` `--workers N` `--batch-size N` `--reindex` `--light-mode` `--enrich` `--chunk-size N` `--chunk-overlap N` `--nrt` `--no-cross-reference` `--keep-books` `--providers [list]` |
| `search` | Performs a search query via CLI | `query` `--top-k N` `--sqlite` `--light-mode` |
| `api` | Starts the FastAPI server | `--host 0.0.0.0` `--port N` `--sqlite` `--light-mode` `--nrt` |

### `boogle-db` - Database Management (Postgres)
| Command | Description |
|---------|-------------|
| `migrate` | Creates necessary tables (`books`, `idx_documents`, `idx_terms`, `idx_globals`) |
| `clear` | Truncates index tables only (`idx_documents`, `idx_terms`, `idx_globals`) |
| `clear-all`| Truncates all tables (Data Reset) |
| `drop` | Drops all tables |
| `test` | Verifies database connection and schema |

---

## 🌐 API Reference

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API information |
| `GET` | `/providers` | List all providers with quality scores |
| `GET` | `/health` | Health check and status |
| `GET` | `/search` | Search books with filters |
| `GET` | `/book/{canonical_id}` | Detailed book information |

### Search Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | string | Search query (required) |
| `limit` | integer | Max results (default: 10, max: 100) |
| `offset` | integer | Pagination offset (default: 0) |
| `sources` | array | Filter by providers |
| `languages` | array | Filter by language codes (e.g., `["en", "pt"]`) |
| `year_from` | integer | Minimum publication year |
| `year_to` | integer | Maximum publication year |
| `subjects` | array | Filter by subjects |
| `min_completeness` | float | Minimum metadata quality (0-1) |

### Example Requests

**Basic search:**
```bash
curl "http://localhost:8000/search?query=shakespeare&limit=5"
```

**Filter by provider:**
```bash
curl "http://localhost:8000/search?query=pride&sources=gutenberg"
```

**Complex search with filters:**
```bash
curl "http://localhost:8000/search?query=novel&languages=en&year_from=1800&year_to=1900&min_completeness=0.8"
```

**Get book details:**
```bash
curl "http://localhost:8000/book/gutenberg:1342"
```

### Response Format

```json
{
  "canonical_id": "gutenberg:1342",
  "title": "Pride and Prejudice",
  "authors": [
    {
      "name": "Jane Austen",
      "role": "author"
    }
  ],
  "language": "en",
  "publication_year": 1813,
  "subjects": ["Fiction", "Love stories"],
  "metadata_completeness": 0.92,
  "primary_source": {
    "provider": "gutenberg",
    "book_id": "1342",
    "url": "https://www.gutenberg.org/ebooks/1342",
    "quality_score": 1.0,
    "files": [
      {
        "format": "txt",
        "url": "https://..."
      }
    ]
  },
  "all_sources": [
    {
      "provider": "gutenberg",
      "book_id": "1342",
      "quality_score": 1.0
    }
  ],
  "source_count": 1,
  "score": 0.95
}
```

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
- **`flush()` now writes to disk properly**: Creates new segment files and updates `index.json`
- Memory buffer is cleared after successful flush
- Reloads disk index automatically

---

## 🔒 Security Layer

Boogle includes comprehensive security validation to protect against common attacks.

### Security Features

| Feature | Description |
|---------|-------------|
| **SQL Injection Detection** | Blocks queries with SQL patterns (`; DROP`, `--`, etc.) |
| **XSS Prevention** | Sanitizes input to prevent cross-site scripting |
| **Path Traversal Blocking** | Prevents directory traversal (`../../`, `..\`, etc.) |
| **Rate Limiting** | Configurable per-IP request limits (default: 100/minute) |
| **Security Headers** | CSP, XSS protection, HSTS, frame options |
| **Input Validation** | Query length limits, character filtering |

### Security Middleware

The API automatically applies security protections on all endpoints.

**Test Security Validators:**
```bash
uv run python << 'EOF'
from src.security.validators import SecurityValidators, SecurityError

# SQL injection blocking
try:
    SecurityValidators.validate_query("'; DROP TABLE books; --")
except SecurityError as e:
    print(f"✓ SQL injection blocked: {e}")

# XSS blocking
try:
    SecurityValidators.validate_query("<script>alert('xss')</script>")
except SecurityError as e:
    print(f"✓ XSS blocked: {e}")

# Valid query
clean = SecurityValidators.validate_query("books about shakespeare")
print(f"✓ Valid query: {clean}")
EOF
```

---

## ⚡ Parallel Provider Seeding

Boogle supports **parallel provider seeding** for faster indexing across multiple sources.

### Enable Parallel Mode

```bash
# Seed from multiple providers in parallel
uv run boogle index --sqlite --parallel --max-parallel-providers 3
```

### Performance Benefits

| Mode | 2 Providers | 3 Providers | 4 Providers |
|------|-------------|-------------|-------------|
| Sequential | 10s | 15s | 20s |
| Parallel (2 threads) | 5s | 8s | 10s |
| Parallel (3 threads) | 4s | 5s | 7s |

### Thread Safety

- **Thread-local database connections**: Each provider gets its own DB connection
- **No race conditions**: Isolated state per provider
- **Automatic cleanup**: Connections closed after provider finishes

### Configuration Options

```bash
--parallel                    # Enable parallel seeding (default: True)
--max-parallel-providers N    # Max concurrent providers (default: 4)
--workers N                  # Download workers per provider (default: 16)
```

---

## 📊 Cross-Reference Merging

When seeding from multiple providers, Boogle automatically detects and merges duplicates.

### How It Works

1. **Detect Duplicates**: Uses canonical identifiers (ISBN, LCCN, etc.)
2. **Merge Metadata**: Combines metadata from all sources
3. **Select Primary**: Highest quality provider becomes primary
4. **Store All Sources**: All sources preserved for downloads

### Example

A book available from both Gutenberg and OpenLibrary:

```json
{
  "canonical_id": "gutenberg:1342|OL7400675M",
  "title": "Pride and Prejudice",
  "authors": [{"name": "Jane Austen"}],
  "primary_source": {
    "provider": "gutenberg",
    "book_id": "1342",
    "quality_score": 1.0,
    "url": "https://www.gutenberg.org/ebooks/1342"
  },
  "all_sources": [
    {
      "provider": "gutenberg",
      "book_id": "1342",
      "quality_score": 1.0
    },
    {
      "provider": "openlibrary",
      "book_id": "OL7400675M",
      "quality_score": 0.9
    }
  ],
  "source_count": 2,
  "metadata_completeness": 0.92
}
```

### Disable Cross-Reference

```bash
uv run boogle index --sqlite --no-cross-reference
```

---

## ✅ Testing

Boogle includes comprehensive test suites for all components.

### Run All Tests

```bash
./test_all.sh
```

### Quick Component Tests

**Test Rust Modules:**
```bash
uv run python -c "from rust_bm25 import FileSearcher, RealTimeIndexer; print('✓ OK')"
```

**Test RealTimeIndexer.flush():**
```bash
rm -rf data/test_rt_index && mkdir -p data/test_rt_index
uv run python << 'EOF'
import json, os
from rust_bm25 import RealTimeIndexer

with open("data/test_rt_index/index.json", "w") as f:
    json.dump({"segments": [], "total_docs": 0, "avgdl": 0.0}, f)

indexer = RealTimeIndexer("data/test_rt_index")
for i in range(3):
    indexer.add_document(f"test document {i} with books", f"book_{i}")
count = indexer.flush()

segments = [f for f in os.listdir("data/test_rt_index") if f.startswith("segment_")]
print(f"✓ Flushed {count} docs, segments: {segments}")

# Verify index.json updated
with open("data/test_rt_index/index.json") as f:
    meta = json.load(f)
    print(f"✓ Total docs in meta: {meta['total_docs']}")
rm -rf data/test_rt_index
EOF
```

**Test API Endpoints:**
```bash
# Start API
USE_SQLITE=1 LIGHT_MODE=1 uv run uvicorn src.api.main:app --port 8000

# Test (in another terminal)
curl http://localhost:8000/ | python -m json.tool          # Root
curl http://localhost:8000/providers | python -m json.tool   # Providers
curl http://localhost:8000/health | python -m json.tool      # Health
curl "http://localhost:8000/search?query=test" | python -m json.tool  # Search
```

### Test Documentation

- **`test_all.sh`**: Complete automated test suite
- **`TEST_COMMANDS.md`**: Individual test commands
- **`COMPREHENSIVE_TEST_GUIDE.md`**: Full testing guide with expected outputs
- **`IMPLEMENTATION_STATUS.md`**: Component status and completion

---

## 📖 CLI Reference

Boogle exposes two main CLI tools: `boogle` (APP) and `boogle-db` (DB Ops).

### `boogle` - Application Pipeline
| Command | Description |Flags|
|---------|-------------|-----|
| `index` | Downloads books and builds the BM25 index | `--limit N`, `--sqlite`, `--workers N`, `--batch-size N`, `--reindex`, `--light-mode`, `--enrich`, `--chunk-size N`, `--chunk-overlap N`, `--nrt`, `--no-cross-reference`, `--keep-books`, `--providers [list]` |
| `search` | Performs a search query via CLI | `query`, `--top-k N`, `--sqlite`, `--light-mode` |
| `api` | Starts the FastAPI server | `--host 0.0.0.0`, `--port N`, `--sqlite`, `--light-mode`, `--nrt` |

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

## 🔌 Multi-Provider System with Unified Schema

Boogle supports multiple book providers through a pluggable architecture with automatic cross-reference merging and quality scoring.

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Providers Layer                      │
├──────────────┬──────────────┬──────────────┬─────────────┤
│  Gutenberg   │ OpenLibrary  │   PPORTAL    │   + More    │
│  (Quality: 1.0)│ (Quality: 0.9)│  (Quality: 0.7)│              │
└──────┬───────┴──────┬───────┴──────┬───────┴──────┬──────┘
       │              │               │              │
       └──────────────┼───────────────┼──────────────┘
                      │               │
              ┌───────▼───────────────▼───────┐
              │       Translator Registry     │
              │     (Unified Schema Mapping)  │
              └───────┬───────────────┬───────┘
                      │               │
              ┌───────▼───────────────▼───────┐
              │    Cross-Reference Service    │
              │    (Duplicate Detection &     │
              │     Metadata Merging)         │
              └───────┬───────────────┬───────┘
                      │               │
              ┌───────▼───────────────▼───────┐
              │        Unified Results        │
              │   (Best Quality as Primary)   │
              └───────────────────────────────┘
```

### Available Providers

| Provider | Description | Downloads | Quality | Default |
|----------|-------------|-----------|---------|---------|
| **gutenberg** | Project Gutenberg | ✅ Yes | 1.0 | ✅ Enabled |
| **openlibrary** | Open Library metadata | ❌ No | 0.9 | ✅ Enabled |
| **pportal** | Portuguese Public Domain | ✅ Yes | 0.7 | ✅ Enabled |

### Unified Schema Features

- **Cross-reference merging**: Automatically detects duplicates across providers
- **Quality scoring**: Higher quality providers become primary sources
- **Canonical IDs**: Unique identifiers spanning multiple sources
- **Source selection**: Download from any available source
- **Metadata enrichment**: Combines metadata from all sources

### Enabling Providers

Providers can be enabled via environment variables:

```bash
# Enable Open Library
export BOOGLE_PROVIDER_OPENLIBRARY_ENABLED=1

# Enable PPORTAL (Portuguese literature)
export BOOGLE_PROVIDER_PPORTAL_ENABLED=1

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
uv run boogle search "machado de assis" --sqlite --source pportal
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

## 📦 Project Status

### ✅ Implemented Features
- ✅ Multi-provider system with unified schema
- ✅ Parallel provider seeding with thread safety
- ✅ Security validators and middleware
- ✅ Cross-reference merging with quality scoring
- ✅ RealTimeIndexer with disk persistence
- ✅ Light mode for metadata-only indexing
- ✅ Full API with search, providers, health, book endpoints
- ✅ SQLite and PostgreSQL support
- ✅ Comprehensive test suite

### 🔧 Configuration
- **Database**: SQLite (default) or PostgreSQL
- **Index Mode**: Batch, Realtime, or Light
- **Multi-provider**: Parallel or sequential indexing
- **Security**: Rate limiting, input validation, headers

### 📝 Documentation
- 📖 [Comprehensive Test Guide](COMPREHENSIVE_TEST_GUIDE.md) - All test commands
- 🧪 [Test Commands](TEST_COMMANDS.md) - Individual component tests
- ✅ [Implementation Status](IMPLEMENTATION_STATUS.md) - Component status
- 🔌 [Adding Providers](ADDING_NEW_PROVIDER.md) - Provider development guide

---

## 🙏 Contributing

Contributions are welcome! The codebase uses:
- **Python 3.13+** for orchestration and API
- **Rust 1.80+** for high-performance indexing
- **FastAPI** for the REST API
- **pytest** for testing

Run tests before submitting:
```bash
./test_all.sh
```

---

## 🪪 License

This project is open-source under the **MIT License**.
Feel free to fork, modify, and improve!

> *Boogle — Free Books. Free Knowledge.*

---

**Built with ❤️ for open access to knowledge**
