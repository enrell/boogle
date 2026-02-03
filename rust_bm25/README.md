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
├── docs/                   # Technical documentation
│   ├── README.md           # System-level overview
│   ├── index/              # Index module docs
│   └── search/             # Search module docs
```

## Python API

### Classes

| Class | Description |
|-------|-------------|
| `FileSearcher` | Search over disk-based segments |
| `RealTimeIndexer` | NRT indexer with WAL |
| `BM25Index` | In-memory index with rkyv serialization |
| `WandSearcher` | WAND algorithm for top-k retrieval |

### Functions

| Function | Description |
|----------|-------------|
| `analyze(text)` | Tokenize and stem Portuguese text |
| `parse_epub(path)` | Extract text from EPUB file |
| `parse_pdf(path)` | Extract text from PDF file |
| `parse_txt(path)` | Read and normalize text file |
| `chunk_text(text, size, overlap)` | Split text into overlapping chunks |
| `encode_postings(postings)` | Compress posting list |
| `decode_postings(data)` | Decompress posting list |
| `merge_postings(a, b)` | Merge two compressed posting lists |
| `index_corpus_file(...)` | Index a directory of books |
| `process_batch(...)` | Process a batch of documents |
| `process_books_to_index(...)` | Parallel book processing |
| `file_hashes_batch(paths)` | Compute MD5 hashes for files |
| `run_streaming_pipeline(...)` | Async download and index pipeline |

### Usage Examples

```python
import rust_bm25

# Text analysis
tokens = rust_bm25.analyze("texto para análise")
# Returns: ['text', 'par', 'analis']

# Parse documents
text = rust_bm25.parse_epub("book.epub")
text = rust_bm25.parse_pdf("document.pdf")
text = rust_bm25.parse_txt("file.txt")

# Chunk text
chunks = rust_bm25.chunk_text(text, chunk_size=1000, overlap=100)

# Index a corpus directory
books, total_chunks = rust_bm25.index_corpus_file(
    books_dir="./books",
    index_dir="./index",
    chunks_dir="./chunks",
    stopwords=["de", "a", "o"],
    chunk_size=1000,
    chunk_overlap=100,
    batch_size=1000
)

# Search over indexed segments
searcher = rust_bm25.FileSearcher("./index")
results = searcher.search("query text", top_k=10)
# Returns: [(book_id, score, doc_id), ...]

# Real-time indexing
rt = rust_bm25.RealTimeIndexer("./index")
doc_id = rt.add_document("document content", '{"title": "My Book"}')
results = rt.search("query", 10)
rt.flush()  # Persist to disk

# In-memory index
index = rust_bm25.BM25Index(k1=1.5, b=0.75)
index.add_document(0, "first document", '{}')
index.add_document(1, "second document", '{}')
index.finalize()
index.save("index.bin")

# WAND searcher (for pre-loaded postings)
wand = rust_bm25.WandSearcher(num_docs=1000, avgdl=500.0)
results = wand.search("query", posting_data, top_k=10)
```

## Segment Files

| File | Contents |
|------|----------|
| `terms.fst` | FST mapping terms to indices |
| `offsets.bin` | Term offset table (28 bytes/term) |
| `postings_docs.bin` | Compressed doc ID deltas |
| `postings_freqs.bin` | Compressed term frequencies |
| `chunks.bin` | Document to book ID mapping |
| `doc_lengths.bin` | Document lengths for BM25 |
| `meta.json` | Segment metadata |

## BM25 Scoring

```
score = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * (docLen / avgdl)))
```

Default parameters: k1=1.5, b=0.75

## Building

```bash
# Development (from project root)
source .venv/bin/activate.fish  # or activate for bash
maturin develop --release -m rust_bm25/Cargo.toml

# Production wheel
maturin build --release -m rust_bm25/Cargo.toml

# Run tests
cargo test --manifest-path rust_bm25/Cargo.toml
```

## Dependencies

- **pyo3**: Python bindings
- **rayon**: Parallel processing
- **fst**: Finite state transducer for term dictionary
- **bitpacking**: SIMD compression
- **memmap2**: Memory-mapped file access
- **serde/rkyv**: Serialization
- **tokio/reqwest**: Async HTTP (pipeline)

## Documentation

See [docs/README.md](./docs/README.md) for detailed system-level documentation including:
- Memory layouts and data flow diagrams
- Threading model and context switching
- Page fault handling with mmap
