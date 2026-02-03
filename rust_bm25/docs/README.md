# rust_bm25 Technical Documentation

This documentation provides system-level insights into how data flows through the rust_bm25 search engine, with emphasis on memory management and context switching.

## Documentation Index

### Index Module
- [segment.rs](./index/segment.md) - Core data structures
- [writer.rs](./index/writer.md) - Segment writing pipeline
- [reader.rs](./index/reader.md) - Memory-mapped segment reading
- [memory.rs](./index/memory.md) - In-memory index operations
- [ram.rs](./index/ram.md) - RAM-based inverted index
- [realtime.rs](./index/realtime.md) - Near real-time indexer
- [wal.rs](./index/wal.md) - Write-ahead log

### Search Module
- [searcher.rs](./search/searcher.md) - File-based BM25 search
- [wand.rs](./search/wand.md) - WAND algorithm implementation

## High-Level Data Flow


### Indexing Pipeline
```
┌───────────────────────────────────────────────────────────────────────────┐
│                         INDEXING PIPELINE                                 │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────┐     ┌───────────┐      ┌────────────┐     ┌────────────┐    │
│  │ Raw Text │───▶│ Tokenizer │───▶ │ Term Freqs │───▶│ Postings   │    │
│  │ (Heap)   │     │ (Arena)   │      │ (HashMap)  │     │ (Encoded)  │    │
│  └──────────┘     └───────────┘      └────────────┘     └────────────┘    │
│                                                            │              │
│                                                            ▼              │
│  ┌──────────────────────────────────────────────────────────────┐         │
│  │                     Segment Files (Disk)                     │         │
│  │  ┌─────────┐ ┌──────────┐ ┌─────────────┐ ┌───────────────┐  │         │
│  │  │terms.fst│ │offsets   │ │postings_docs│ │postings_freqs │  │         │
│  │  └─────────┘ └──────────┘ └─────────────┘ └───────────────┘  │         │
│  └──────────────────────────────────────────────────────────────┘         │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

### Search Pipeline

```
┌───────────────────────────────────────────────────────────────────────────┐
│                          SEARCH PIPELINE                                  │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────┐     ┌───────────┐      ┌────────────┐     ┌────────────┐    │
│  │  Query   │───▶│ Tokenizer │───▶ │ FST Lookup │───▶│ Postings   │    │
│  │ (Stack)  │     │ (Heap)    │      │ (mmap)     │     │ Iterator   │    │
│  └──────────┘     └───────────┘      └────────────┘     └──────┬─────┘    │
│                                                                │              │
│   ┌────────────────────────────────────────────────────────────┘              │
│   │                                                                           │
│   ▼                                                                           │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────────┐       │
│  │ BM25 Scoring   │───▶│ Top-K Select   │───▶│ Results (Vec)      │       │
│  │ (Stack/Heap)   │    │ (Heap Min-Heap)│    │ (Heap)             │       │
│  └────────────────┘    └────────────────┘    └────────────────────┘       │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

## Memory Regions Overview

| Region | Usage | Lifetime |
|--------|-------|----------|
| Stack | Function locals, small buffers, iterators | Function scope |
| Heap | Documents, term maps, result vectors | Explicit allocation |
| Arena (Bump) | Tokenization scratch space | Batch scope |
| mmap | Segment files (terms.fst, postings) | Process lifetime |
| OS Page Cache | Recently accessed mmap pages | Managed by OS |

## Thread Model

```
Main Thread (Python GIL)
    │
    ├──▶ index_corpus_file()
    │         │
    │         ├──▶ Rayon Thread Pool (CPU-bound)
    │         │         ├── Document parsing
    │         │         ├── Tokenization
    │         │         └── Posting encoding
    │         │
    │         └──▶ Writer Thread (I/O-bound)
    │                   └── Segment writing
    │
    └──▶ search()
              │
              └──▶ Sequential (mmap reads)
                        └── BM25 scoring
```

## Context Switching Points

| Switch Type | Location | Overhead |
|-------------|----------|----------|
| Python ↔ Rust | `py.detach()` | ~1μs |
| Rayon Work Stealing | `par_iter()` | ~100ns |
| mmap Page Faults | Any mmap access | ~1μs + I/O |
| Channel Send/Recv | Bounded channels | Blocks when full |
| Lock Acquisition | `RwLock`, `Mutex` | ~50ns uncontended |
