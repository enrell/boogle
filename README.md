# 📚 Boogle

Open-source search engine for free books. Search across public-domain collections with modern relevance ranking.

## Quick Start

```bash
# Start database
docker compose up -d db

# Download books and metadata
uv run boogle seed --limit 500

# Build search index
uv run boogle index

# Start API
uv run uvicorn src.api.main:app --reload
```

Visit `http://127.0.0.1:8000/docs` to try the API.

## Commands

| Command | Description |
|---------|-------------|
| `boogle seed` | Download books from Gutenberg |
| `boogle seed --refresh` | Update metadata for existing books |
| `boogle index` | Build/update search index |
| `boogle search "query"` | Search from CLI |

## Data Sources

- [x] Project Gutenberg
- [ ] Open Library
- [ ] Internet Archive

## License

MIT
