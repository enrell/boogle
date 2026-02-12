# Boogle Enhanced Search

Advanced search capabilities with multilingual support, spell correction, and query expansion.

## Features

### 1. Multilingual Language Detection

Automatically detects the language of search queries using the **Lingua** library (supports 75+ languages).

**Supported Languages:**
- English (en)
- Portuguese (pt) - Critical for PPORTAL
- Spanish (es)
- French (fr)
- German (de)
- Italian (it)
- Dutch (nl)
- Russian (ru)
- Chinese (zh)
- Japanese (ja)
- Arabic (ar)
- Hindi (hi)

**Example:**
```bash
# Portuguese query auto-detected
curl "http://localhost:8000/search/enhanced?query=dom+casmurro"
# Response: language detected: pt
```

### 2. Spell Correction

Uses **SymSpell** (1000x faster than Levenshtein distance) to automatically correct typos.

**Features:**
- Prefix indexing for O(1) lookup
- Configurable edit distance (default: 2)
- Works with custom dictionaries

**Example:**
```bash
# Typo automatically corrected
curl "http://localhost:8000/search/enhanced?query=shakspeare"
# Corrected to: shakespeare
```

### 3. Query Expansion with Synonyms

Expands queries with synonyms using **NLTK WordNet** to improve recall.

**Expansion Sources:**
- NLTK WordNet for English synonyms
- Book-specific domain terms (novel→story/fiction, author→writer/poet)
- Language-specific terms for Portuguese (livro→obra, autor→escritor)

**Example:**
```bash
# Query "author" expands to include: writer, poet, novelist, playwright
curl "http://localhost:8000/search/enhanced?query=author&expand=true"
```

### 4. Multi-Language Stopwords

Uses the project's `stopwords-iso.json` (50+ languages) instead of English-only stopwords.

**Supported:** English, Portuguese, Spanish, French, German, Italian, Dutch, Russian, and 40+ more.

## API Endpoints

### GET /search/enhanced

Enhanced search with NLP features.

**Parameters:**
- `query` (required): Search query string
- `limit` (optional): Number of results (default: 10, max: 100)
- `explain` (optional): Return detailed explanation (default: false)
- `spellcheck` (optional): Enable spell correction (default: true)
- `expand` (optional): Enable query expansion (default: true)

**Example Response (explain=true):**
```json
{
  "original_query": "shakspeare novel",
  "processed_query": "shakespeare novel",
  "search_query": "shakespeare novel OR tragedy OR play OR drama",
  "language": "en",
  "enhancements": {
    "spell_correction": {
      "applied": true,
      "corrections": {
        "shakspeare": "shakespeare"
      },
      "corrected_query": "shakespeare novel"
    },
    "query_expansion": {
      "applied": true,
      "expansions": {
        "novel": ["tragedy", "play", "drama"]
      },
      "expanded_query": "shakespeare novel OR tragedy OR play OR drama"
    },
    "language_detection": {
      "detected": "en",
      "name": "English"
    }
  },
  "results": [
    {
      "book_id": "1524",
      "title": "Hamlet",
      "author": "William Shakespeare",
      "score": 15.4321
    }
  ]
}
```

## Usage Examples

### Basic Enhanced Search
```bash
# Default: spell correction + expansion enabled
curl "http://localhost:8000/search/enhanced?query=machado+de+assiz"
```

### Disable Spell Correction
```bash
curl "http://localhost:8000/search/enhanced?query=shakspeare&spellcheck=false"
```

### Disable Query Expansion
```bash
curl "http://localhost:8000/search/enhanced?query=author&expand=false"
```

### Portuguese Search
```bash
# Auto-detects Portuguese, uses PT stopwords
curl "http://localhost:8000/search/enhanced?query=memórias+póstumas"
```

### Get Detailed Explanation
```bash
curl "http://localhost:8000/search/enhanced?query=autor&explain=true"
```

## Python API

```python
from src.search import EnhancedSearcher

# Initialize with all features
searcher = EnhancedSearcher(
    index_dir="data/index",
    enable_spellcheck=True,
    enable_expansion=True,
    enable_language_detection=True,
    expansion_boost=0.3,
)

# Search with enhancements
results, query_info = searcher.search(
    "shakspeare novel",
    top_k=10,
    apply_spellcheck=True,
    apply_expansion=True,
)

# Get explanation
explanation = searcher.search_with_explanation("shakspeare", top_k=5)
print(explanation)
```

## Performance

- **Spell Correction**: O(1) lookup with prefix index
- **Language Detection**: ~1ms per query
- **Query Expansion**: ~5ms per word
- **Caching**: Results cached for 5 minutes by default

## Testing

Run the test suite:

```bash
# Test all enhanced search features
uv run python3 test_enhanced_search.py

# Test expansion module specifically
uv run python3 -m pytest tests/search/test_expansion.py -v
```

## Architecture

```
Query → Language Detection → Spell Correction → Query Expansion → BM25 Search
         ↓                      ↓                    ↓
    Lingua Library         SymSpell          NLTK WordNet
    (75+ languages)       (1000x speed)     (Synonyms)
```

## Configuration

Environment variables:
- `ENHANCED_SEARCH=1`: Enable enhanced search (default: enabled)
- `INDEX_DIR=data/index`: BM25 index directory

## Troubleshooting

**Q: Language detection not working?**
A: Check that `lingua-language-detector` is installed: `pip install lingua-language-detector`

**Q: Spell correction not suggesting words?**
A: Spell corrector needs a dictionary. Add words using `SpellCorrector.add_words()` or use `SpellCorrector.from_corpus()`.

**Q: Query expansion returning empty results?**
A: Check that NLTK WordNet is downloaded: The module auto-downloads it on first use.

## Future Improvements

- [ ] Support for more languages in query expansion
- [ ] Integration with multilingual WordNets
- [ ] Neural query rewriting
- [ ] Cross-lingual search (search in English, find Portuguese results)