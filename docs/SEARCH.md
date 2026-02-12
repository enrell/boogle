# Boogle Search System

Complete guide to Boogle's search capabilities, architecture, and implementation.

## Table of Contents

1. [Overview](#overview)
2. [Search Architecture](#search-architecture)
3. [Query Processing Pipeline](#query-processing-pipeline)
4. [Indexing System](#indexing-system)
5. [Ranking & Scoring](#ranking--scoring)
6. [Semantic Chunking](#semantic-chunking)
7. [Multilingual Support](#multilingual-support)
8. [Query Enhancements](#query-enhancements)
9. [Performance Optimization](#performance-optimization)

---

## Overview

Boogle provides a complete search solution with:

- **BM25 Ranking**: Industry-standard relevance scoring
- **Semantic Chunking**: Content-aware text splitting
- **Multilingual**: 75+ languages with automatic detection
- **Query Enhancements**: Spell correction + synonym expansion
- **Real-time Indexing**: Add books without rebuilding
- **Hybrid Search**: Metadata + full-text search

---

## Search Architecture

```
Query → Preprocessing → Language Detection → Spell Correction → Query Expansion
                                           ↓
BM25 Index ← Postings Lists ← Segments ← Chunks ← Books
                                           ↓
                Scoring → Ranking → Deduplication → Results
```

### Components

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Index Engine | Rust BM25 | Fast inverted index with SIMD compression |
| Chunking | Rust | Content-aware text splitting |
| Language Detection | Lingua | 75+ language detection |
| Spell Correction | SymSpell | 1000x faster than Levenshtein |
| Query Expansion | NLTK WordNet | Synonym expansion |
| Storage | SQLite/PostgreSQL | Metadata and checkpoints |

---

## Query Processing Pipeline

### 1. Input Validation

```python
# Security validation
query = SecurityValidators.validate_query(query)
# - Sanitizes input
# - Prevents injection attacks
# - Removes harmful characters
```

### 2. Language Detection

Automatically detects query language:

```python
from src.search.language import LanguageDetector

detector = LanguageDetector()
lang = detector.detect("O Brasil é um país maravilhoso")  # Returns: "pt"
confidence = detector.detect_with_confidence("Hello world")  # Returns: ("en", 0.98)
```

**Supported Languages:**
- European: en, pt, es, fr, de, it, nl, ru
- Asian: zh, ja, ko, ar, hi
- And 65+ more

### 3. Spell Correction

Automatically fixes typos:

```python
# Input: "shakspeare"
# Corrected: "shakespeare"

# Input: "machado de assiz"
# Corrected: "machado de assis"
```

**Features:**
- Edit distance: 2 (configurable)
- Prefix indexing: O(1) lookup
- Custom dictionary support

### 4. Query Expansion

Expands queries with synonyms:

```python
# Input: "author"
# Expands to: "author OR writer OR novelist OR poet OR playwright"

# Input: "novel"
# Expands to: "novel OR story OR fiction OR book OR tale"
```

**Sources:**
- NLTK WordNet (English)
- Domain-specific terms (book, author, etc.)
- Language-specific terms (Portuguese)

### 5. BM25 Search

Core search algorithm:

```
score(D, Q) = Σ IDF(q) * (f(q,D) * (k1 + 1)) / (f(q,D) + k1 * (1 - b + b * |D|/avg_dl))

Where:
- D = document
- Q = query
- q = query term
- f(q,D) = term frequency in document
- IDF = inverse document frequency
- k1, b = tuning parameters
```

**Parameters:**
- k1 = 1.5 (term frequency saturation)
- b = 0.75 (document length normalization)

---

## Indexing System

### Index Structure

```
data/index/
├── index.json              # Index metadata
├── segment_0/              # Segment 0
│   ├── postings_docs.bin   # Document IDs (BitPacked + VarInt)
│   ├── postings_freqs.bin  # Term frequencies
│   ├── terms.fst          # FST term dictionary
│   ├── chunks.bin         # Text chunks
│   └── doc_lengths.bin    # Document lengths
├── segment_1/
└── ...
```

### Compression

| Component | Method | Ratio |
|-----------|--------|-------|
| Posting Lists | BitPacking + VarInt | 60-70% |
| Term Dictionary | FST | 40-50% |
| Text Chunks | Zstandard (zstd) | 60-70% |

### Indexing Modes

**1. Batch Indexing (Default)**
```bash
# Indexes all books at once
uv run boogle index --sqlite --reindex
```

**2. NRT (Near Real-Time)**
```bash
# Incremental updates
uv run boogle index --sqlite --nrt
```

**3. Light Mode**
```bash
# Metadata only (no full text)
uv run boogle index --sqlite --light-mode
```

---

## Ranking & Scoring

### BM25 Scoring

```rust
// Score calculation
fn bm25_score(
    term_freq: u32,
    doc_length: u32,
    avg_doc_length: f32,
    total_docs: u32,
    docs_with_term: u32,
) -> f32 {
    let idf = ((total_docs as f32 - docs_with_term as f32 + 0.5)
        / (docs_with_term as f32 + 0.5))
        .ln();
    
    let tf_component = (term_freq as f32 * (K1 + 1.0))
        / (term_freq as f32 + K1 * (1.0 - B + B * doc_length as f32 / avg_doc_length));
    
    idf * tf_component
}
```

### Cross-Reference Merging

When multiple providers have the same book:

1. **Deduplication**: Canonical ID matching
2. **Quality Scoring**: Source quality (Gutenberg: 1.0, OpenLibrary: 0.9)
3. **Metadata Merging**: Combine fields from all sources
4. **Best Source Selection**: Pick highest quality for download

**Example:**
```
Provider A: Title="Hamlet", Author="Shakespeare", Quality=0.8
Provider B: Title="Hamlet", Author="William Shakespeare", Quality=0.9

Merged:
  Title: "Hamlet"
  Author: "William Shakespeare" (from higher quality)
  Quality: 0.9
  Sources: [A, B]
```

---

## Semantic Chunking

### Why Semantic Chunking?

**Problem with fixed-size chunks:**
```
Fixed: "The quick brown fox jumps over the lazy..."
       "...dog and runs into the forest."
       
Split mid-sentence! Bad for search.
```

**Solution - Hierarchical chunking:**
```
Chapter → Paragraph → Sentence → Fixed
```

### Chunk Boundary Types

**1. Chapter Detection**
```python
# Detects: CHAPTER, # Chapter, BOOK, PART, SECTION
text = """CHAPTER 1
The Beginning

Content here...

CHAPTER 2
The End"""

chunks = chunk_by_structure(text, "chapter")
# Returns: ["CHAPTER 1\nThe Beginning\n\nContent...", "CHAPTER 2\nThe End"]
```

**2. Paragraph Splitting**
```python
# Splits on \n\n (blank lines)
chunks = chunk_by_structure(text, "paragraph")
```

**3. Sentence Splitting**
```python
# Handles abbreviations: Mr., Dr., St., etc.
text = "Mr. Smith and Dr. Jones went to St. Louis."
chunks = chunk_by_structure(text, "sentence")
# Returns: ["Mr. Smith and Dr. Jones went to St. Louis."]
# NOT: ["Mr.", "Smith and Dr.", "Jones went to St.", "Louis."]
```

**4. Smart Semantic Chunking**
```python
from rust_bm25 import chunk_text_semantic

# Automatic hierarchy: Chapter → Paragraph → Sentence → Fixed
chunks = chunk_text_semantic(text, target_size=500, overlap=50)
```

**Algorithm:**
1. Try to split by chapters
2. If chapter too long → split by paragraphs
3. If paragraph too long → split by sentences
4. If sentence too long → use fixed-size

### Performance Impact

| Metric | Fixed-Size | Semantic | Improvement |
|--------|-----------|----------|-------------|
| Mid-sentence splits | 40% | 5% | -87% |
| Mid-paragraph splits | 60% | 15% | -75% |
| Search relevance | baseline | +8-15% | ✓ |

---

## Multilingual Support

### Language Detection

```python
from src.search.language import LanguageDetector

detector = LanguageDetector()

# Detect language
detector.detect("Hello world")           # "en"
detector.detect("Olá mundo")             # "pt"
detector.detect("Bonjour le monde")     # "fr"

# With confidence
detector.detect_with_confidence("Hola")
# Returns: ("es", 0.95)
```

### Multi-Language Stopwords

```python
from src.indexer.stopwords import load_stopwords, get_stopwords_for_language

# Get English stopwords
en_stopwords = get_stopwords_for_language("en")

# Get Portuguese stopwords
pt_stopwords = get_stopwords_for_language("pt")

# Load multiple languages
all_stopwords = load_stopwords(["en", "pt", "es", "fr"])
```

**Supported:** 50+ languages via `stopwords-iso.json`

### Language-Specific Features

**Portuguese (PPORTAL):**
- Book synonyms: livro→obra, autor→escritor
- Stopwords: o, a, os, as, etc.

**English:**
- WordNet synonyms
- Book domain terms

---

## Query Enhancements

### Spell Correction

```python
from src.search.spellcheck import SpellCorrector

corrector = SpellCorrector()

# Correct single word
corrector.correct_word("shakspeare")  # "shakespeare"

# Correct full query
corrected, was_corrected, corrections = corrector.correct_query("shakspeare novel")
# Returns: ("shakespeare novel", True, {"shakspeare": "shakespeare"})
```

### Query Expansion

```python
from src.search.expansion import BookQueryExpander

expander = BookQueryExpander()

# Expand query
expanded, info = expander.expand_query("author writes novel")
# expanded: "author writes novel OR writer OR novelist OR book OR story"
# info: {"author": ["writer", "novelist"], "novel": ["book", "story"]}
```

### Combined Pipeline

```python
from src.search import EnhancedSearcher

searcher = EnhancedSearcher()

results, query_info = searcher.search(
    "shakspeare novel",
    apply_spellcheck=True,
    apply_expansion=True,
)

# Results include:
# - Spell-corrected: "shakespeare novel"
# - Expanded: "shakespeare novel OR tragedy OR play"
# - Language detected: "en"
```

---

## Performance Optimization

### Caching

```python
from src.search import CachedEnhancedSearcher

searcher = CachedEnhancedSearcher(
    cache_size=1000,
    cache_ttl=300,  # 5 minutes
)

# First query: computed
# Subsequent queries: cached
results = searcher.search("popular query")
```

### Index Optimization

**Segment Size:**
- Target: 1000-5000 documents per segment
- Too small: overhead
- Too large: slow queries

**Chunk Size:**
- Default: 1000 characters
- Overlap: 100 characters
- Semantic: 500-2000 characters (adaptive)

### Disk Usage

| Component | Size (per 1000 books) |
|-----------|----------------------|
| Source files (.txt) | ~5 GB |
| Compressed chunks (.zst) | ~1.5 GB |
| BM25 Index | ~500 MB |
| Database | ~100 MB |
| **Total (with cleanup)** | **~2 GB** |
| **Total (keep books)** | **~7 GB** |

**Recommendation:** Use `--keep-books=false` (default) for 70% space savings.

---

## Configuration

### Environment Variables

```bash
# Database
export USE_SQLITE=1                    # Use SQLite (default)
export DATABASE_URL="postgresql://..." # PostgreSQL

# Indexing
export INDEX_DIR="data/index"          # BM25 index location
export CHUNKS_DIR="data/chunks"        # Compressed chunks

# Search
export ENHANCED_SEARCH=1               # Enable enhancements (default: 1)
export LIGHT_MODE=0                    # Metadata only
export REALTIME_INDEX=0                # NRT mode

# Chunking
export CHUNK_SIZE=1000                 # Characters per chunk
export CHUNK_OVERLAP=100               # Overlap between chunks
```

### Tuning Parameters

**BM25 Parameters:**
- `k1`: 1.2-2.0 (higher = more term frequency influence)
- `b`: 0.5-0.9 (higher = more length normalization)

**Chunking Parameters:**
- `target_size`: 500-2000 (characters)
- `min_size`: 200-500
- `max_size`: 1500-3000

---

## Troubleshooting

### Low Search Relevance

**Check:**
1. Are you using semantic chunking?
   ```bash
   # Reindex with semantic chunking
   uv run boogle index --sqlite --reindex
   ```

2. Is query expansion enabled?
   ```python
   # Check in response
   response["enhancements"]["query_expanded"]
   ```

3. Are stopwords filtered?
   ```python
   from src.indexer.stopwords import load_stopwords
   stopwords = load_stopwords(["en", "pt"])
   ```

### Slow Queries

**Solutions:**
1. Reduce segment size
2. Enable caching
3. Use Light Mode for metadata-only queries
4. Check if index is on fast storage (SSD)

### High Memory Usage

**Solutions:**
1. Use batch indexing instead of NRT
2. Reduce batch_size
3. Enable streaming cleanup
4. Use SQLite instead of PostgreSQL (lower overhead)

---

## Examples

### Basic Search

```bash
curl "http://localhost:8000/search?query=machado+de+assis&limit=5"
```

### With Language Detection

```bash
curl "http://localhost:8000/search?query=memórias+póstumas"
# Response: {"language_detected": "pt", ...}
```

### With Spell Correction

```bash
curl "http://localhost:8000/search?query=shakspeare"
# Response: {"spell_corrected": true, "corrected_query": "shakespeare", ...}
```

### With Filters

```bash
curl "http://localhost:8000/search?query=novel&sources=gutenberg&languages=en&year_from=1900&year_to=2000"
```

---

## References

- [BM25 Paper](https://www.cs.otago.ac.nz/home/staff/om/cosc462/resources/WP-07-01.pdf)
- [Lingua Language Detection](https://github.com/pemistahl/lingua-py)
- [SymSpell](https://github.com/wolfgarbe/SymSpell)
- [WordNet](https://wordnet.princeton.edu/)
- [FST](https://docs.rs/fst/latest/fst/)