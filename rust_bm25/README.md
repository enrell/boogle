# rust_bm25

High-performance BM25 full-text search engine with Python bindings.

## Architecture

```
rust_bm25/
├── src/
│   ├── lib.rs              # PyO3 module exports
│   ├── analysis/           # Text tokenization and stemming
│   ├── codecs/             # Posting list compression (BitPacking + VarInt)
│   ├── document/           # Document parsing (EPUB, PDF, TXT)
│   ├── index/              # Index structures
│   │   ├── segment.rs      # Core data types
│   │   ├── writer.rs       # Segment writer
│   │   ├── reader.rs       # Memory-mapped segment reader
│   │   ├── memory.rs       # In-memory index (Python API)
│   │   ├── ram.rs          # RAM-based inverted index
│   │   ├── realtime.rs     # Near real-time indexer
│   │   └── wal.rs          # Write-ahead log
│   ├── search/             # Search implementations
│   │   ├── searcher.rs     # File-based BM25 searcher
│   │   └── wand.rs         # WAND algorithm searcher
│   └── pipeline.rs         # Async indexing pipeline
```

## Components

### Analysis

**`analyze(text) -> Vec<String>`**

Tokenizes and stems Portuguese text:
1. Converts Unicode to ASCII (deunicode)
2. Lowercases text
3. Splits on non-alphabetic characters
4. Filters tokens (2-25 chars)
5. Applies Portuguese stemmer

### Codecs

Efficient posting list compression:
- **BitPacking**: 128-value SIMD blocks for bulk data
- **VarInt**: Tail elements and legacy format
- Delta encoding for document IDs

### Index

**Segment Files:**
| File | Contents |
|------|----------|
| `terms.fst` | FST mapping terms to indices |
| `offsets.bin` | Term offset table (28 bytes/term) |
| `postings_docs.bin` | Compressed doc ID deltas |
| `postings_freqs.bin` | Compressed term frequencies |
| `chunks.bin` | Document to book ID mapping |
| `doc_lengths.bin` | Document lengths for BM25 |
| `meta.json` | Segment metadata |

### Search

**BM25 Scoring:**
```
score = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (docLen / avgdl)))
```

Default parameters: k1=1.5, b=0.75

**Features:**
- Fuzzy matching via Levenshtein automaton
- Streaming postings iterator (zero-copy)
- Top-K selection with quickselect

### Pipeline

Three-stage async processing:
1. **Downloader**: Fetches content (async, semaphore-limited)
2. **Processor**: Parses, chunks, and analyzes (parallel threads)
3. **Indexer**: Writes segments (single writer thread)

## Python API

```python
import rust_bm25

# Text analysis
tokens = rust_bm25.analyze("texto para análise")

# Index a corpus
books, chunks = rust_bm25.index_corpus_file(
    books_dir="./books",
    index_dir="./index",
    chunks_dir="./chunks",
    stopwords=["de", "a", "o"],
    chunk_size=1000,
    chunk_overlap=100,
    batch_size=1000
)

# Search
searcher = rust_bm25.FileSearcher("./index")
results = searcher.search("query", top_k=10)
# Returns: [(book_id, score, doc_id), ...]

# Real-time indexing
rt = rust_bm25.RealTimeIndexer("./index")
doc_id = rt.add_document("content", '{"title": "Book"}')
results = rt.search("query", 10)
rt.flush()
```

## Building

```bash
# Development
maturin develop --release

# Production wheel
maturin build --release
```

## Dependencies

- **pyo3**: Python bindings
- **rayon**: Parallel processing
- **fst**: Finite state transducer for term dictionary
- **bitpacking**: SIMD compression
- **memmap2**: Memory-mapped file access
- **serde/rkyv**: Serialization
- **tokio/reqwest**: Async HTTP (pipeline)
