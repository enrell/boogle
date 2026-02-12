# Quick API Test Commands

Curl commands to test the Boogle API.

## Prerequisites

API server must be running:
```bash
python -m src.api.main
# or
./run.sh api
```

## Test Commands

### List Books with Pagination
```bash
curl -s "http://localhost:8000/books?limit=3&offset=0" | python -m json.tool
```

### List Books Filtered by Source
```bash
# Filter by provider (gutenberg, openlibrary, pportal, internetarchive)
curl -s "http://localhost:8000/books?source=gutenberg&limit=3" | python -m json.tool
```

### Search Books
```bash
curl -s "http://localhost:8000/search?query=shakespeare&limit=3" | python -m json.tool
```

### Get Book Details
```bash
curl -s "http://localhost:8000/book/gutenberg:1" | python -m json.tool
```

### List Available Providers
```bash
curl -s "http://localhost:8000/providers" | python -m json.tool
```

### Health Check
```bash
curl -s "http://localhost:8000/health" | python -m json.tool
```

## Response Fields

### Files Field
The `files` field in responses shows available download formats:

```json
{
  "primary_source": {
    "provider": "pportal",
    "book_id": "123",
    "url": "https://example.com/book/123",
    "files": [
      {
        "format": "pdf",
        "url": "https://example.com/book/123.pdf",
        "size": "2.5 MB"
      },
      {
        "format": "epub",
        "url": "https://example.com/book/123.epub"
      }
    ]
  }
}
```

**Note**: Some providers (like gutenberg) may not store file format information in the database, resulting in `files: null`. This is expected behavior when the source data doesn't include download links.

### Available Formats
Supported file formats: `pdf`, `epub`, `txt`, `mobi`, `html`

## Advanced Search with Filters

```bash
# Search with filters
curl -s "http://localhost:8000/search?query=history&source=gutenberg&language=en&limit=5" | python -m json.tool

# Filter by multiple formats
curl -s "http://localhost:8000/search?query=book&formats=pdf&formats=epub&limit=5" | python -m json.tool
```
