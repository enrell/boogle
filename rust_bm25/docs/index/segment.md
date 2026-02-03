# segment.rs - Core Data Structures

## Purpose
Defines the fundamental data types used throughout the indexing pipeline. These structures are the "contracts" between different stages of processing.

## Data Structures

### SegmentMeta
```rust
pub struct SegmentMeta {
    pub num_docs: u32,      // 4 bytes
    pub base_doc_id: u32,   // 4 bytes
    pub total_length: u64,  // 8 bytes
}
```

**Memory Layout**: 16 bytes, stack-allocated when used locally.

**Serialization**: Written to `meta.json` as JSON (human-readable, ~50-100 bytes).

**Purpose**: Stored per-segment to enable:
- Document ID mapping (global ↔ local)
- Average document length calculation for BM25

---

### IndexMeta
```rust
pub struct IndexMeta {
    pub segments: Vec<String>,  // Heap: 24 bytes + N * string_len
    pub total_docs: u32,        // 4 bytes
    pub avgdl: f32,             // 4 bytes
}
```

**Memory Layout**: 32 bytes on stack + heap allocation for segment names.

**Serialization**: Written to `index.json` at index root.

**Purpose**: Global index metadata for:
- Segment discovery during search
- Pre-computed `avgdl` for BM25 scoring

---

### ProcessedDoc
```rust
pub struct ProcessedDoc {
    pub book_id: String,                           // Heap: 24 + len
    pub chunks: Vec<(u32, FxHashMap<String, u32>)> // Heap: complex
}
```

**Memory Layout**:
```
Stack (ProcessedDoc):
├── book_id: String (24 bytes ptr+len+cap)
└── chunks: Vec (24 bytes ptr+len+cap)

Heap:
├── book_id data: [u8; len]
└── chunks array:
    └── For each chunk:
        ├── u32 doc_length (4 bytes)
        └── FxHashMap:
            ├── Buckets array (power of 2 size)
            └── Entry: (String key, u32 value)
```

**Typical Size**: 1KB-10KB per document depending on chunk count.

**Lifetime**: Created in processor threads, consumed by writer thread.

---

### BatchData
```rust
pub struct BatchData {
    pub segment_id: usize,   // 8 bytes
    pub segment_dir: PathBuf, // Heap: 24 + path_len
    pub docs: Vec<ProcessedDoc>, // Heap: large
    pub base_doc_id: u32,    // 4 bytes
}
```

**Memory Layout**: ~60 bytes on stack + significant heap allocation.

**Purpose**: Bundles all data needed to write one segment.

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    ProcessedDoc Creation                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Parse file → String (heap allocated, ~100KB-10MB)           │
│                    │                                             │
│  2. chunk_text()   ▼                                             │
│     ┌──────────────────────────────────────────┐                │
│     │ Vec<String> chunks                        │                │
│     │ Each chunk: ~1KB text                     │                │
│     └──────────────────────────────────────────┘                │
│                    │                                             │
│  3. analyze_arena() │  (Arena allocator - fast, bulk dealloc)   │
│                    ▼                                             │
│     ┌──────────────────────────────────────────┐                │
│     │ Vec<&str> tokens (pointers into arena)   │                │
│     └──────────────────────────────────────────┘                │
│                    │                                             │
│  4. Build freq map │                                             │
│                    ▼                                             │
│     ┌──────────────────────────────────────────┐                │
│     │ FxHashMap<String, u32>                   │                │
│     │ Keys: owned strings (copied from arena)  │                │
│     │ Values: term frequencies                 │                │
│     └──────────────────────────────────────────┘                │
│                    │                                             │
│  5. Bundle into    │                                             │
│     ProcessedDoc   ▼                                             │
│     ┌──────────────────────────────────────────┐                │
│     │ ProcessedDoc {                           │                │
│     │   book_id: "abc123",                     │                │
│     │   chunks: [(len, freq_map), ...]         │                │
│     │ }                                        │                │
│     └──────────────────────────────────────────┘                │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Memory Ownership

| Structure | Owner | Transfer |
|-----------|-------|----------|
| ProcessedDoc | Processor thread | Moved to channel |
| BatchData | Main thread | Moved to writer thread |
| SegmentMeta | Writer thread | Serialized to disk |
| IndexMeta | Main thread | Serialized to disk |

## Thread Safety

All structures are `Send` (can be transferred between threads) because:
- They contain only owned data
- No `Rc`, `RefCell`, or raw pointers
- HashMap uses `FxHashMap` which is `Send`
