# Boogle Developer Guide

Welcome to the Boogle development documentation. This guide provides detailed instructions on setting up the environment, running the system components, and executing the test suite.

## 1. Prerequisites

- **Python**: Version 3.13 or higher.
- **Package Manager**: `uv` (recommended) or `pip`.
- **Database**: 
  - SQLite (Default, easiest for development)
  - PostgreSQL (Recommended for production or full integration testing)

## 2. Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/enrell/boogle.git
    cd boogle
    ```

2.  **Install dependencies using `uv`**:
    ```bash
    uv sync
    ```
    Or with pip:
    ```bash
    pip install -e .
    ```

## 3. Database Setup

Boogle supports both SQLite and PostgreSQL.

### SQLite (Development)
No special setup is required. The application defaults to using a local file at `data/boogle.db`.

To explicitly run with SQLite:
```bash
export USE_SQLITE=1
```

### PostgreSQL (Production/Advanced Dev)
1.  Ensure PostgreSQL is running.
2.  Create a database (e.g., `boogle`).
3.  Set the `DATABASE_URL` environment variable:
    ```bash
    export DATABASE_URL="postgresql://user:password@localhost:5432/boogle"
    export USE_SQLITE=0
    ```

### Migrations
The project uses **Alembic** for schema migrations.
To apply the latest schema:
```bash
uv run alembic upgrade head
```

## 4. Running the System

The `boogle` command is the main entry point (installed via `project.scripts`).

### A. Indexing Pipeline
The pipeline consists of Scraper -> Downloader -> Indexer.

1.  **Seed and Index**:
    Downloads books from Gutenberg and builds the BM25 index.
    ```bash
    # Download 100 books and index them
    uv run boogle index --limit 100 --sqlite
    ```
    
    *Options*:
    - `--limit <N>`: Number of books to check/download.
    - `--reindex`: Force deleting and rebuilding the index files.
    - `--workers <N>`: Number of threads for downloading (default 16).

2.  **Search CLI**:
    Test the index directly from the command line.
    ```bash
    uv run boogle search "founding fathers" --sqlite
    ```

### B. API Server
Start the FastAPI server to serve search results.

```bash
uv run boogle api --sqlite --host 0.0.0.0 --port 8000
```
API Documentation will be available at: `http://localhost:8000/docs`

## 5. Testing

We use **pytest** for testing.

### Running Unit Tests
```bash
uv run pytest
```

### Running Integration Tests
We have a dedicated sequential integration pipeline that tests the full flow (Scrape -> Download -> Index -> Search -> API) using a real network connection and a temporary SQLite database.

**Warning**: This test downloads data from the internet.

```bash
uv run pytest tests/test_integration_pipeline.py -s -v
```
* The `-s` flag allows you to see the progress output.
* The `-v` flag provides verbose results.

### Test Environment Variables
The integration tests automatically handle these, but if you need to manually test with separate data:

- `BOOKS_DIR`: Directory to store downloaded books.
- `INDEX_DIR`: Directory to store the search index.
- `SQLITE_DB_PATH`: Path to the SQLite database file.

## 6. Project Architecture

- **`src/scraper`**: Handles fetching metadata and file links from Project Gutenberg.
- **`src/downloader`**: Downloads book content (txt/epub) and manages deduplication.
- **`src/indexer`**:  Uses `rust_bm25` (custom Rust extension) to build inverted indices.
- **`src/db`**: Database models (SQLAlchemy) and repository layer.
- **`src/api`**: FastAPI application serving the React frontend.
- **`scripts/`**: Utility scripts (e.g., metadata enrichment).

## 7. Troubleshooting

**"Books directory should not be empty" in tests**:
This usually means the scraper couldn't reach Gutenberg or network requests timed out. Check your internet connection or try increasing the timeout/limit in the test file.

**Database Locked (SQLite)**:
Ensure only one process (indexer or API) is writing to the database at a time, or enable WAL mode.
