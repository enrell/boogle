# Integration Tests

## End-to-End Pipeline Test

The `tests/test_integration_pipeline.py` script runs a full end-to-end test of the Boogle indexing and search pipeline, including:
1.  Seeding (downloading) books.
2.  Indexing.
3.  Searching (Baseline).
4.  Enrichment (Mock Open Library data).
5.  Searching (Enhanced) and comparing scores.

### Running with SQLite (Local)
To run the test using a temporary SQLite database (no Docker required):
```bash
uv run pytest tests/test_integration_pipeline.py -s -v --sqlite
```

### Running with PostgreSQL (Docker)
To run the test using the running PostgreSQL instance (default):
```bash
uv run pytest tests/test_integration_pipeline.py -s -v
```

### Note
The test seeds a small number of books (5) to ensure speed.
Enrichment is verified by checking if the search score for "Project Gutenberg" (or similar top hit) increases after enrichment data is applied.
