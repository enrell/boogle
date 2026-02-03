# memory.rs - In-Memory Index Operations

## Purpose
Provides a pure in-memory BM25 index with `rkyv` serialization. Used for small indexes that fit in RAM or as a building block for hybrid indexes.

## BM25Index Structure

```rust
struct IndexData {
    k1: f32,                                    // 4 bytes
    b: f32,                                     // 4 bytes
    terms: HashMap<String, Vec<u8>>,            // Heap: complex
    term_df: HashMap<String, u32>,              // Heap: complex
    doc_lengths: Vec<u32>,                      // Heap: 4 * num_docs
    doc_metadata: Vec<String>,                  // Heap: varies
    num_docs: u32,                              // 4 bytes
    avgdl: f32,                                 // 4 bytes
}

pub struct BM25Index {
    data: IndexData,                            // See above
    pending: FxHashMap<String, Vec<(u32, u32)>> // Heap: staging area
}
```

## Memory Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│                         BM25Index MEMORY                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Stack (BM25Index):                                                  │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ data: IndexData (inline, ~100 bytes)                     │       │
│  │ pending: FxHashMap (24 bytes ptr)                        │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  Heap (IndexData.terms):                                            │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ HashMap buckets array (power of 2)                       │       │
│  │  └── Entry: (hash, String key, Vec<u8> encoded postings) │       │
│  │                                                           │       │
│  │ Example for "hello" with 1000 postings:                  │       │
│  │  Key: "hello" (5 + 24 = 29 bytes)                        │       │
│  │  Value: ~2KB compressed postings                         │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  Heap (IndexData.doc_lengths):                                      │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ Contiguous array: [u32; num_docs]                        │       │
│  │ 1M docs = 4MB                                            │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  Heap (IndexData.doc_metadata):                                     │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ Vec of String pointers → individual heap allocations     │       │
│  │ Fragmented memory, consider arena for production         │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  Heap (pending):                                                    │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ Staging area for documents not yet finalized             │       │
│  │ Term → [(doc_id, freq), ...]                             │       │
│  │ Grows during add_document(), cleared on finalize()       │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Two-Phase Commit Pattern

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DOCUMENT INSERTION FLOW                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Phase 1: add_document() - O(tokens)                                │
│  ┌───────────────────────────────────────────────────────────┐      │
│  │ 1. Tokenize text → Vec<String>                            │      │
│  │ 2. Build FxHashMap<&str, u32> (term → freq)               │      │
│  │ 3. Append to pending: term → [(doc_id, freq), ...]        │      │
│  │ 4. Store doc_length and metadata                          │      │
│  │                                                            │      │
│  │ Memory: ~O(unique_tokens) per document                    │      │
│  │ No encoding, no compression                               │      │
│  └───────────────────────────────────────────────────────────┘      │
│                                                                      │
│  Phase 2: finalize() - O(total_postings)                            │
│  ┌───────────────────────────────────────────────────────────┐      │
│  │ 1. Compute avgdl from doc_lengths                         │      │
│  │ 2. For each term in pending:                              │      │
│  │    - Encode postings → Vec<u8>                            │      │
│  │    - Merge with existing (if any)                         │      │
│  │    - Update term_df                                       │      │
│  │ 3. Clear pending HashMap                                  │      │
│  │                                                            │      │
│  │ Memory: Postings compressed ~4x                           │      │
│  │ High CPU for encoding                                     │      │
│  └───────────────────────────────────────────────────────────┘      │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Why two phases?**
- Batching improves encoding efficiency
- Delta encoding works better with sorted doc IDs
- User controls when expensive work happens

---

## Serialization with rkyv

### Save Flow
```
save(path)
    │
    ▼
rkyv::to_bytes(&self.data)
    │
    ├── Zero-copy serialization
    │   (structures written as-is where possible)
    │
    ├── Relative pointers replace absolute
    │
    └── Output: Vec<u8> (same layout as in-memory)
            │
            ▼
        fs::write(path, bytes)
            │
            └── Single sequential write
```

### Load Flow
```
load(path)
    │
    ▼
fs::read(path) → Vec<u8>
    │
    ▼
rkyv::access::<ArchivedIndexData>(&bytes)
    │
    ├── Validation (optional, skipped for speed)
    │
    └── Returns reference into bytes
            │
            ▼
rkyv::deserialize()
    │
    ├── Copies data to owned structures
    │
    └── Returns IndexData
```

**Memory during load**:
- File size × 2 briefly (file buffer + deserialized)
- After: Only deserialized data

---

## Parallel Processing Functions

### `process_books_to_index()`

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PARALLEL BOOK PROCESSING                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Input: Vec<(path, metadata)>                                       │
│                                                                      │
│  Rayon par_iter()                                                   │
│  ┌───────────────────────────────────────────────────────────┐      │
│  │ Thread 0          │ Thread 1          │ Thread N          │      │
│  │ ┌───────────────┐ │ ┌───────────────┐ │ ┌───────────────┐ │      │
│  │ │ parse book A  │ │ │ parse book B  │ │ │ parse book N  │ │      │
│  │ │ tokenize      │ │ │ tokenize      │ │ │ tokenize      │ │      │
│  │ │ build freqs   │ │ │ build freqs   │ │ │ build freqs   │ │      │
│  │ └───────────────┘ │ └───────────────┘ │ └───────────────┘ │      │
│  └───────────────────────────────────────────────────────────┘      │
│                              │                                       │
│                              ▼                                       │
│  ┌───────────────────────────────────────────────────────────┐      │
│  │ merge_results()                                           │      │
│  │ - Combine all term maps (sequential, single thread)       │      │
│  │ - Collect all doc records                                 │      │
│  └───────────────────────────────────────────────────────────┘      │
│                              │                                       │
│                              ▼                                       │
│  Output: (docs, term_postings, total_length)                        │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

**Thread-Local State**:
- Each thread has its own `FxHashMap` for term frequencies
- `AtomicU32` for global doc ID assignment
- No locks during parsing (embarrassingly parallel)

**Merge Phase**:
- Single-threaded to avoid HashMap contention
- Could be parallelized with concurrent HashMap

---

## Memory Estimates

| Documents | Tokens/Doc | Index RAM |
|-----------|------------|-----------|
| 10K | 1000 | ~100MB |
| 100K | 1000 | ~1GB |
| 1M | 1000 | ~10GB |

**Breakdown**:
- Postings: ~60% (compressed)
- Term strings: ~20%
- Doc metadata: ~15%
- Overhead: ~5%
