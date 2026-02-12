# Boogle API Reference

Complete API documentation for frontend developers integrating with Boogle.

**Base URL:** `http://localhost:8000`

**API Version:** 2.1.0

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Authentication](#authentication)
3. [Endpoints](#endpoints)
   - [GET /](#get-)
   - [GET /search](#get-search)
   - [GET /providers](#get-providers)
   - [GET /book/{canonical_id}](#get-bookcanonical_id)
   - [GET /health](#get-health)
4. [Request/Response Formats](#requestresponse-formats)
5. [Error Handling](#error-handling)
6. [Rate Limiting](#rate-limiting)
7. [Examples](#examples)

---

## Quick Start

### 1. Start the API Server

```bash
# Using SQLite (easiest)
USE_SQLITE=1 uv run uvicorn src.api.main:app --port 8000

# Using PostgreSQL
uv run uvicorn src.api.main:app --port 8000
```

### 2. Test the API

```bash
# Check if API is running
curl http://localhost:8000/

# Search for books
curl "http://localhost:8000/search?query=shakespeare&limit=5"
```

### 3. Interactive Documentation

OpenAPI (Swagger) UI available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

---

## Authentication

Currently, the Boogle API does not require authentication. However, this may change in future versions.

**Note:** Always validate inputs on your frontend to prevent injection attacks.

---

## Endpoints

### GET /

API root and information.

**URL:** `GET /`

**Response:**

```json
{
  "message": "Boogle Search API",
  "version": "2.1.0",
  "features": [
    "multi-provider",
    "unified-schema",
    "source-selection",
    "language-detection",
    "spell-correction",
    "query-expansion",
    "semantic-chunking"
  ]
}
```

**Status Codes:**
- `200 OK`: Success

---

### GET /search

Main search endpoint with NLP enhancements (spell correction, query expansion, language detection).

**URL:** `GET /search`

**Query Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `query` | string | Yes | - | Search query (1-1000 characters) |
| `limit` | integer | No | 10 | Number of results (1-100) |
| `offset` | integer | No | 0 | Pagination offset (≥0) |
| `sources` | array[string] | No | null | Filter by providers (e.g., `gutenberg`, `openlibrary`, `pportal`) |
| `languages` | array[string] | No | null | Filter by language codes (e.g., `en`, `pt`, `es`) |
| `year_from` | integer | No | null | Filter by minimum publication year (1000-2100) |
| `year_to` | integer | No | null | Filter by maximum publication year (1000-2100) |
| `subjects` | array[string] | No | null | Filter by subjects/categories |
| `min_completeness` | float | No | 0.0 | Minimum metadata completeness (0.0-1.0) |

**Response Format:**

```json
{
  "results": [
    {
      "canonical_id": "gutenberg:1524",
      "title": "Hamlet",
      "subtitle": null,
      "authors": [
        {
          "name": "William Shakespeare",
          "role": "author"
        }
      ],
      "language": "en",
      "subjects": ["Drama", "Tragedy"],
      "publication_year": 1603,
      "cover_url": "https://example.com/cover.jpg",
      "thumbnail_url": "https://example.com/thumb.jpg",
      "metadata_completeness": 0.95,
      "primary_source": {
        "provider": "gutenberg",
        "book_id": "1524",
        "url": "https://www.gutenberg.org/ebooks/1524",
        "files": [
          {
            "format": "txt",
            "url": "https://www.gutenberg.org/files/1524/1524-0.txt"
          }
        ]
      },
      "all_sources": [
        {
          "provider": "gutenberg",
          "book_id": "1524",
          "url": "https://www.gutenberg.org/ebooks/1524",
          "files": [...]
        },
        {
          "provider": "openlibrary",
          "book_id": "OL12345W",
          "url": "https://openlibrary.org/works/OL12345W",
          "files": [...]
        }
      ],
      "source_count": 2,
      "score": 15.4321
    }
  ],
  "meta": {
    "total": 42,
    "limit": 10,
    "offset": 0,
    "query": "shakespeare",
    "original_query": "shakspeare"
  },
  "enhancements": {
    "language_detected": "en",
    "spell_corrected": true,
    "query_expanded": true,
    "spell_corrections": {
      "shakspeare": "shakespeare"
    },
    "expansions": {
      "author": ["writer", "playwright", "poet"]
    }
  }
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `results` | array | List of search results |
| `results[].canonical_id` | string | Unique book identifier (format: `provider:book_id`) |
| `results[].title` | string | Book title |
| `results[].subtitle` | string \| null | Book subtitle |
| `results[].authors` | array | List of authors with roles |
| `results[].authors[].name` | string | Author name |
| `results[].authors[].role` | string | Author role (e.g., "author", "editor") |
| `results[].language` | string \| null | ISO 639-1 language code |
| `results[].subjects` | array \| null | List of subjects/categories |
| `results[].publication_year` | integer \| null | Publication year |
| `results[].cover_url` | string \| null | Cover image URL |
| `results[].thumbnail_url` | string \| null | Thumbnail image URL |
| `results[].metadata_completeness` | float | Metadata completeness score (0.0-1.0) |
| `results[].primary_source` | object | Best quality source |
| `results[].all_sources` | array | All available sources |
| `results[].source_count` | integer | Number of sources |
| `results[].score` | float | BM25 relevance score |
| `meta.total` | integer | Total number of matching results |
| `meta.limit` | integer | Requested limit |
| `meta.offset` | integer | Requested offset |
| `meta.query` | string | Corrected/normalized query |
| `meta.original_query` | string | Original user query |
| `enhancements.language_detected` | string \| null | Detected language code |
| `enhancements.spell_corrected` | boolean | Whether spell correction was applied |
| `enhancements.query_expanded` | boolean | Whether query expansion was applied |
| `enhancements.spell_corrections` | object \| null | Map of original → corrected words |
| `enhancements.expansions` | object \| null | Map of terms → expanded terms |

**Status Codes:**
- `200 OK`: Success
- `400 Bad Request`: Invalid query (too long, contains invalid characters)
- `500 Internal Server Error`: Server error

**Example Requests:**

**Basic Search:**
```bash
curl "http://localhost:8000/search?query=machado+de+assis&limit=5"
```

**Search with Filters:**
```bash
curl "http://localhost:8000/search?query=novel&sources=gutenberg&languages=en&year_from=1900&year_to=2000&limit=10"
```

**Search with Typo (shows spell correction):**
```bash
curl "http://localhost:8000/search?query=shakspeare"
# Response shows: spell_corrections: {"shakspeare": "shakespeare"}
```

**Multilingual Search (Portuguese):**
```bash
curl "http://localhost:8000/search?query=memórias+póstumas"
# Response shows: language_detected: "pt"
```

---

### GET /providers

List all available book providers.

**URL:** `GET /providers`

**Response:**

```json
{
  "providers": [
    {
      "name": "gutenberg",
      "quality_score": 1.0,
      "supports_downloads": true
    },
    {
      "name": "openlibrary",
      "quality_score": 0.9,
      "supports_downloads": true
    },
    {
      "name": "pportal",
      "quality_score": 0.7,
      "supports_downloads": true
    }
  ]
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `providers` | array | List of providers |
| `providers[].name` | string | Provider identifier |
| `providers[].quality_score` | float | Quality rating (0.0-1.0) |
| `providers[].supports_downloads` | boolean | Whether full-text is available |

**Status Codes:**
- `200 OK`: Success

---

### GET /book/{canonical_id}

Get detailed information about a specific book.

**URL:** `GET /book/{canonical_id}`

**Path Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `canonical_id` | string | Yes | Book identifier (format: `provider:book_id`) |

**Response:**

```json
{
  "canonical_id": "gutenberg:1524",
  "title": "Hamlet",
  "subtitle": "Prince of Denmark",
  "authors": [
    {
      "name": "William Shakespeare",
      "role": "author"
    }
  ],
  "language": "en",
  "subjects": ["Drama", "Tragedy", "English literature"],
  "publication_year": 1603,
  "description": "The Tragedy of Hamlet, Prince of Denmark...",
  "cover_url": "https://example.com/cover.jpg",
  "thumbnail_url": "https://example.com/thumb.jpg",
  "page_count": null,
  "isbn_10": null,
  "isbn_13": null,
  "primary_source": {
    "provider": "gutenberg",
    "book_id": "1524",
    "url": "https://www.gutenberg.org/ebooks/1524",
    "files": [
      {
        "format": "txt",
        "url": "https://www.gutenberg.org/files/1524/1524-0.txt"
      },
      {
        "format": "epub",
        "url": "https://www.gutenberg.org/ebooks/1524.epub"
      }
    ]
  },
  "all_sources": [
    {
      "provider": "gutenberg",
      "book_id": "1524",
      "url": "https://www.gutenberg.org/ebooks/1524",
      "files": [
        {"format": "txt", "url": "..."},
        {"format": "epub", "url": "..."}
      ]
    },
    {
      "provider": "openlibrary",
      "book_id": "OL12345W",
      "url": "https://openlibrary.org/works/OL12345W",
      "files": [
        {"format": "pdf", "url": "..."}
      ]
    }
  ],
  "source_count": 2,
  "metadata_completeness": 0.95,
  "indexed_at": "2025-02-11T10:30:00"
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `canonical_id` | string | Unique identifier |
| `title` | string | Book title |
| `subtitle` | string \| null | Subtitle |
| `authors` | array | Authors with roles |
| `language` | string \| null | Language code |
| `subjects` | array \| null | Subjects |
| `publication_year` | integer \| null | Year published |
| `description` | string \| null | Book description/summary |
| `cover_url` | string \| null | Cover image |
| `thumbnail_url` | string \| null | Thumbnail |
| `page_count` | integer \| null | Number of pages |
| `isbn_10` | string \| null | ISBN-10 |
| `isbn_13` | string \| null | ISBN-13 |
| `primary_source` | object | Best source for download |
| `all_sources` | array | All sources with download links |
| `source_count` | integer | Number of sources |
| `metadata_completeness` | float | Completeness score |
| `indexed_at` | string | ISO 8601 timestamp |

**Status Codes:**
- `200 OK`: Success
- `404 Not Found`: Book not found

**Example:**
```bash
curl "http://localhost:8000/book/gutenberg:1524"
```

---

### GET /health

Health check endpoint.

**URL:** `GET /health`

**Response:**

```json
{
  "status": "healthy",
  "mode": "batch",
  "providers": ["gutenberg", "openlibrary", "pportal"]
}
```

**Field Descriptions:**

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | "healthy" or "unhealthy" |
| `mode` | string | Indexing mode: "batch", "realtime", or "light" |
| `providers` | array | Available providers |

**Status Codes:**
- `200 OK`: Healthy
- `503 Service Unavailable`: Unhealthy

---

## Request/Response Formats

### Content Type

All endpoints accept and return **JSON**.

**Request Headers:**
```
Content-Type: application/json
Accept: application/json
```

### Date/Time Format

All dates are in **ISO 8601** format:
```
2025-02-11T10:30:00
```

### Language Codes

Use **ISO 639-1** codes:
- `en`: English
- `pt`: Portuguese
- `es`: Spanish
- `fr`: French
- `de`: German
- etc.

### Pagination

Use `limit` and `offset` for pagination:

```
Page 1: offset=0, limit=10
Page 2: offset=10, limit=10
Page 3: offset=20, limit=10
```

---

## Error Handling

### Error Response Format

```json
{
  "detail": "Error message here"
}
```

### Common Errors

| Status Code | Meaning | Common Causes |
|-------------|---------|---------------|
| `400` | Bad Request | Invalid query, missing parameters |
| `404` | Not Found | Book doesn't exist |
| `429` | Too Many Requests | Rate limit exceeded |
| `500` | Server Error | Database error, indexing issue |
| `503` | Service Unavailable | Server not ready |

### Handling Errors (JavaScript Example)

```javascript
async function searchBooks(query) {
  try {
    const response = await fetch(
      `http://localhost:8000/search?query=${encodeURIComponent(query)}`
    );
    
    if (!response.ok) {
      if (response.status === 404) {
        console.log("No results found");
        return [];
      }
      if (response.status === 429) {
        console.log("Rate limit exceeded, retrying...");
        await sleep(1000);
        return searchBooks(query);
      }
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    
    return await response.json();
  } catch (error) {
    console.error("Search failed:", error);
    throw error;
  }
}
```

---

## Rate Limiting

**Current Limits:**
- No strict rate limiting implemented
- Recommend: Max 100 requests/minute per client

**Future:** Rate limiting may be added. Check response headers:
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1707657600
```

---

## Examples

### React Example

```jsx
import { useState, useEffect } from 'react';

function BookSearch() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const searchBooks = async () => {
    if (!query.trim()) return;
    
    setLoading(true);
    setError(null);
    
    try {
      const response = await fetch(
        `http://localhost:8000/search?query=${encodeURIComponent(query)}&limit=10`
      );
      
      if (!response.ok) {
        throw new Error('Search failed');
      }
      
      const data = await response.json();
      setResults(data.results);
      
      // Show spell correction if applied
      if (data.enhancements?.spell_corrected) {
        console.log(`Did you mean: ${data.meta.corrected_query}?`);
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Search books..."
      />
      <button onClick={searchBooks} disabled={loading}>
        {loading ? 'Searching...' : 'Search'}
      </button>
      
      {error && <div className="error">{error}</div>}
      
      <div className="results">
        {results.map(book => (
          <div key={book.canonical_id} className="book">
            <h3>{book.title}</h3>
            <p>by {book.authors.map(a => a.name).join(', ')}</p>
            <p>Score: {book.score.toFixed(2)}</p>
            <a href={`/book/${book.canonical_id}`}>View Details</a>
          </div>
        ))}
      </div>
    </div>
  );
}
```

### Vue.js Example

```vue
<template>
  <div>
    <input v-model="query" @keyup.enter="search" placeholder="Search books..." />
    <button @click="search" :disabled="loading">Search</button>
    
    <div v-if="loading">Loading...</div>
    <div v-if="error" class="error">{{ error }}</div>
    
    <div v-if="correction" class="correction">
      Did you mean: <a @click="query = correction; search()">{{ correction }}</a>?
    </div>
    
    <div class="results">
      <div v-for="book in results" :key="book.canonical_id" class="book">
        <h3>{{ book.title }}</h3>
        <p>{{ book.authors.map(a => a.name).join(', ') }}</p>
        <p>Language: {{ book.language }}</p>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  data() {
    return {
      query: '',
      results: [],
      loading: false,
      error: null,
      correction: null
    };
  },
  methods: {
    async search() {
      this.loading = true;
      this.error = null;
      this.correction = null;
      
      try {
        const res = await fetch(
          `http://localhost:8000/search?query=${encodeURIComponent(this.query)}`
        );
        const data = await res.json();
        
        this.results = data.results;
        
        if (data.enhancements?.spell_corrected) {
          this.correction = data.meta.corrected_query;
        }
      } catch (err) {
        this.error = err.message;
      } finally {
        this.loading = false;
      }
    }
  }
};
</script>
```

### Python Example

```python
import requests

def search_books(query, limit=10):
    """Search books with error handling."""
    url = "http://localhost:8000/search"
    params = {
        "query": query,
        "limit": limit
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Show spell correction
        if data.get("enhancements", {}).get("spell_corrected"):
            print(f"Did you mean: {data['meta']['corrected_query']}?")
        
        return data["results"]
    
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return []

# Usage
results = search_books("shakespeare", limit=5)
for book in results:
    print(f"{book['title']} by {book['authors'][0]['name']}")
```

---

## Changelog

### v2.1.0 (Current)
- Added NLP enhancements (spell correction, query expansion, language detection)
- Added semantic chunking support
- `/search` now returns enhancement metadata
- Added `/health` endpoint

### v2.0.0
- Initial API release
- Multi-provider support
- Unified schema

---

## Support

For questions or issues:
- GitHub Issues: https://github.com/enrell/boogle/issues
- Documentation: https://github.com/enrell/boogle/tree/main/docs

---

## License

MIT License - See LICENSE file for details