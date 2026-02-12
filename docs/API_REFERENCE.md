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

**Provider Filters:**
| `sources` | array[string] | No | null | Include only these providers (e.g., `gutenberg`, `openlibrary`, `pportal`) |
| `exclude_sources` | array[string] | No | null | Exclude these providers |

**Content Filters:**
| `languages` | array[string] | No | null | Filter by language codes (ISO 639-1, e.g., `en`, `pt`, `es`) |
| `formats` | array[string] | No | null | Filter by available formats (`txt`, `epub`, `pdf`, `html`) |
| `has_fulltext` | boolean | No | null | Only books with downloadable full text |

**Quality Filters:**
| `min_completeness` | float | No | 0.0 | Minimum metadata completeness (0.0-1.0) |
| `min_rating` | float | No | 0.0 | Minimum rating (0.0-5.0) |

**Temporal Filters:**
| `year_from` | integer | No | null | Minimum publication year (1000-2100) |
| `year_to` | integer | No | null | Maximum publication year (1000-2100) |

**Subject Filters:**
| `subjects` | array[string] | No | null | Filter by subjects/categories |
| `subject_mode` | string | No | "any" | Subject matching mode: `any`, `all`, or `exact` |

**Result Filters:**
| `deduplicate` | boolean | No | true | Remove duplicate books across providers |

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

**Advanced Filter Examples:**

```bash
# Filter by provider and exclude another
# Only Gutenberg, exclude PPORTAL
curl "http://localhost:8000/search?query=shakespeare&sources=gutenberg&exclude_sources=pportal"

# Filter by language (Portuguese)
curl "http://localhost:8000/search?query=machado&languages=pt"

# Filter by format (only EPUB)
curl "http://localhost:8000/search?query=novel&formats=epub"

# Filter by availability (only books with full text)
curl "http://localhost:8000/search?query=history&has_fulltext=true"

# Filter by quality (high metadata completeness)
curl "http://localhost:8000/search?query=science&min_completeness=0.8"

# Filter by rating (4+ stars)
curl "http://localhost:8000/search?query=fiction&min_rating=4.0"

# Filter by publication year range
curl "http://localhost:8000/search?query=classic&year_from=1800&year_to=1900"

# Subject filtering with mode
# Any of these subjects
curl "http://localhost:8000/search?query=love&subjects=Romance&subjects=Fiction&subject_mode=any"

# All subjects must match
curl "http://localhost:8000/search?query=history&subjects=History&subjects=Biography&subject_mode=all"

# Exclude deduplication (show all sources separately)
curl "http://localhost:8000/search?query=hamlet&deduplicate=false"

# Combined advanced filters
curl "http://localhost:8000/search?query=novel&sources=gutenberg&languages=en&year_from=1900&year_to=1950&subjects=Fiction&min_completeness=0.9&has_fulltext=true&limit=20"
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

## Building an Advanced Filter UI

Here's a complete example of building an advanced search interface with filters:

### React Component with Filters

```jsx
import { useState, useEffect, useCallback } from 'react';

function AdvancedBookSearch() {
  // Search state
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);
  
  // Filter state
  const [filters, setFilters] = useState({
    sources: [],
    exclude_sources: [],
    languages: [],
    formats: [],
    has_fulltext: null,
    min_completeness: 0.0,
    min_rating: 0.0,
    year_from: '',
    year_to: '',
    subjects: [],
    subject_mode: 'any',
    deduplicate: true,
  });
  
  // Available options
  const [providers, setProviders] = useState([]);
  const [languages] = useState([
    { code: 'en', name: 'English' },
    { code: 'pt', name: 'Portuguese' },
    { code: 'es', name: 'Spanish' },
    { code: 'fr', name: 'French' },
    { code: 'de', name: 'German' },
  ]);
  const [formats] = useState([
    { value: 'txt', label: 'Plain Text' },
    { value: 'epub', label: 'EPUB' },
    { value: 'pdf', label: 'PDF' },
    { value: 'html', label: 'HTML' },
  ]);

  // Fetch providers on mount
  useEffect(() => {
    fetch('http://localhost:8000/providers')
      .then(res => res.json())
      .then(data => setProviders(data.providers));
  }, []);

  // Build query string from filters
  const buildQueryString = useCallback(() => {
    const params = new URLSearchParams();
    params.append('query', query);
    params.append('limit', '20');
    
    // Provider filters
    if (filters.sources.length > 0) {
      filters.sources.forEach(s => params.append('sources', s));
    }
    if (filters.exclude_sources.length > 0) {
      filters.exclude_sources.forEach(s => params.append('exclude_sources', s));
    }
    
    // Content filters
    if (filters.languages.length > 0) {
      filters.languages.forEach(l => params.append('languages', l));
    }
    if (filters.formats.length > 0) {
      filters.formats.forEach(f => params.append('formats', f));
    }
    if (filters.has_fulltext !== null) {
      params.append('has_fulltext', filters.has_fulltext.toString());
    }
    
    // Quality filters
    if (filters.min_completeness > 0) {
      params.append('min_completeness', filters.min_completeness.toString());
    }
    if (filters.min_rating > 0) {
      params.append('min_rating', filters.min_rating.toString());
    }
    
    // Temporal filters
    if (filters.year_from) {
      params.append('year_from', filters.year_from);
    }
    if (filters.year_to) {
      params.append('year_to', filters.year_to);
    }
    
    // Subject filters
    if (filters.subjects.length > 0) {
      filters.subjects.forEach(s => params.append('subjects', s));
      params.append('subject_mode', filters.subject_mode);
    }
    
    // Result filters
    if (!filters.deduplicate) {
      params.append('deduplicate', 'false');
    }
    
    return params.toString();
  }, [query, filters]);

  // Search handler
  const search = useCallback(async () => {
    if (!query.trim()) return;
    
    setLoading(true);
    
    try {
      const queryString = buildQueryString();
      const response = await fetch(
        `http://localhost:8000/search?${queryString}`
      );
      
      if (!response.ok) throw new Error('Search failed');
      
      const data = await response.json();
      setResults(data.results);
      setTotal(data.meta.total);
      
    } catch (err) {
      console.error('Search error:', err);
    } finally {
      setLoading(false);
    }
  }, [query, buildQueryString]);

  // Update filter handler
  const updateFilter = (key, value) => {
    setFilters(prev => ({ ...prev, [key]: value }));
  };

  // Toggle array filter (for checkboxes)
  const toggleArrayFilter = (key, value) => {
    setFilters(prev => ({
      ...prev,
      [key]: prev[key].includes(value)
        ? prev[key].filter(v => v !== value)
        : [...prev[key], value]
    }));
  };

  return (
    <div className="advanced-search">
      {/* Search Input */}
      <div className="search-input">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && search()}
          placeholder="Search books..."
        />
        <button onClick={search} disabled={loading}>
          {loading ? 'Searching...' : 'Search'}
        </button>
      </div>

      {/* Filters Panel */}
      <div className="filters-panel">
        {/* Provider Filters */}
        <div className="filter-group">
          <h4>Providers</h4>
          {providers.map(p => (
            <label key={p.name}>
              <input
                type="checkbox"
                checked={filters.sources.includes(p.name)}
                onChange={() => toggleArrayFilter('sources', p.name)}
              />
              {p.name} (quality: {p.quality_score})
            </label>
          ))}
        </div>

        {/* Language Filter */}
        <div className="filter-group">
          <h4>Languages</h4>
          <select
            multiple
            value={filters.languages}
            onChange={(e) => {
              const selected = Array.from(e.target.selectedOptions, o => o.value);
              updateFilter('languages', selected);
            }}
          >
            {languages.map(l => (
              <option key={l.code} value={l.code}>{l.name}</option>
            ))}
          </select>
        </div>

        {/* Format Filter */}
        <div className="filter-group">
          <h4>Formats</h4>
          {formats.map(f => (
            <label key={f.value}>
              <input
                type="checkbox"
                checked={filters.formats.includes(f.value)}
                onChange={() => toggleArrayFilter('formats', f.value)}
              />
              {f.label}
            </label>
          ))}
        </div>

        {/* Quality Filters */}
        <div className="filter-group">
          <h4>Quality</h4>
          <label>
            Min. Completeness: {filters.min_completeness}
            <input
              type="range"
              min="0"
              max="1"
              step="0.1"
              value={filters.min_completeness}
              onChange={(e) => updateFilter('min_completeness', parseFloat(e.target.value))}
            />
          </label>
          <label>
            Min. Rating: {filters.min_rating}
            <input
              type="range"
              min="0"
              max="5"
              step="0.5"
              value={filters.min_rating}
              onChange={(e) => updateFilter('min_rating', parseFloat(e.target.value))}
            />
          </label>
        </div>

        {/* Year Range */}
        <div className="filter-group">
          <h4>Publication Year</h4>
          <input
            type="number"
            placeholder="From"
            value={filters.year_from}
            onChange={(e) => updateFilter('year_from', e.target.value)}
          />
          <input
            type="number"
            placeholder="To"
            value={filters.year_to}
            onChange={(e) => updateFilter('year_to', e.target.value)}
          />
        </div>

        {/* Full Text Only */}
        <div className="filter-group">
          <label>
            <input
              type="checkbox"
              checked={filters.has_fulltext === true}
              onChange={(e) => updateFilter('has_fulltext', e.target.checked ? true : null)}
            />
            Only books with full text
          </label>
        </div>

        {/* Deduplication */}
        <div className="filter-group">
          <label>
            <input
              type="checkbox"
              checked={filters.deduplicate}
              onChange={(e) => updateFilter('deduplicate', e.target.checked)}
            />
            Remove duplicates
          </label>
        </div>
      </div>

      {/* Results */}
      <div className="results">
        <div className="results-header">
          <span>{total} results found</span>
        </div>
        {results.map(book => (
          <div key={book.canonical_id} className="book-card">
            <h3>{book.title}</h3>
            <p>{book.authors?.map(a => a.name).join(', ')}</p>
            <p>{book.publication_year} • {book.language}</p>
            <p>Score: {book.score?.toFixed(2)}</p>
            {book.subjects && (
              <div className="subjects">
                {book.subjects.slice(0, 3).map(s => (
                  <span key={s} className="subject-tag">{s}</span>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

export default AdvancedBookSearch;
```

### Filter UI Best Practices

1. **Collapsible Sections**: Group filters into collapsible sections
2. **Clear Filters**: Add a "Clear All" button
3. **Active Filter Badges**: Show selected filters as removable badges
4. **URL Sync**: Sync filters with URL params for shareable searches
5. **Debounced Search**: Auto-search after filter changes (with debounce)
6. **Loading States**: Show loading indicators
7. **Empty States**: Show message when no results match filters

### Example: Syncing with URL

```jsx
import { useSearchParams } from 'react-router-dom';

function SyncedFilters() {
  const [searchParams, setSearchParams] = useSearchParams();
  
  // Read filters from URL
  const sources = searchParams.getAll('sources');
  const yearFrom = searchParams.get('year_from');
  
  // Update URL when filters change
  const updateUrlFilters = (newFilters) => {
    const params = new URLSearchParams();
    newFilters.sources.forEach(s => params.append('sources', s));
    if (newFilters.year_from) params.set('year_from', newFilters.year_from);
    setSearchParams(params);
  };
  
  // ... rest of component
}
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