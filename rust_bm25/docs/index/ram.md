# ram.rs - RAM-Based Inverted Index

## Purpose
Simple uncompressed in-memory inverted index for real-time indexer. Optimized for fast inserts.

## Structures

```rust
pub struct Document {
    pub id: u32,          // 4 bytes
    pub content: String,  // 24 + len bytes
    pub metadata: String, // 24 + len bytes
    pub length: u32,      // 4 bytes (token count)
}

pub struct RamIndex {
    pub inverted_index: FxHashMap<String, Vec<(u32, u32)>>, // term → [(doc_id, freq)]
    pub docs: FxHashMap<u32, Document>,                      // doc_id → Document
    pub next_doc_id: u32,
    pub total_length: u64,
}
```

## Memory Layout

```
RamIndex (80 bytes stack)
├── inverted_index → Heap HashMap
│   └── Entry: term_string → Vec<(doc_id, freq)>
└── docs → Heap HashMap
    └── Entry: doc_id → Document (with content copy)
```

## Insert Flow

1. `next_doc_id++` → assign ID
2. `analyze(&content)` → tokenize to `Vec<String>`
3. Build `FxHashMap<&str, u32>` term frequencies
4. `docs.insert(doc_id, Document{...})` - clones content
5. For each (term, freq): `inverted_index.entry(term).push((doc_id, freq))`

**Memory**: ~2x content size per document

## Search Flow

1. Tokenize query
2. For each token: lookup postings, compute BM25 score
3. Aggregate in `FxHashMap<u32, f32>`
4. Sort by score descending

## BM25 Constants
- K1 = 1.2, B = 0.75

## clear() Behavior
- Drops all heap data
- Retains HashMap capacity
- Preserves `next_doc_id` (avoids ID collision)
