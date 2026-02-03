# writer.rs - Segment Writing Pipeline

## Purpose
Orchestrates the complete indexing pipeline from raw files to on-disk segments. This is the most complex module in terms of threading and memory management.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                         INDEXING ARCHITECTURE                        │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Main Thread                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ 1. Collect book files (glob)                                    │ │
│  │ 2. Spawn writer thread                                          │ │
│  │ 3. For each batch:                                              │ │
│  │    - Parallel process (Rayon)                                   │ │
│  │    - Send BatchData through channel                             │ │
│  │ 4. Wait for writer thread                                       │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│         │                                                            │
│         │ crossbeam::bounded(1)                                      │
│         ▼                                                            │
│  Writer Thread                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ Loop: recv BatchData                                            │ │
│  │   - write_segment()                                             │ │
│  │   - Accumulate SegmentMeta results                              │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Thread Communication

### Channel: `bounded::<BatchData>(1)`

**Why bounded(1)?**
- Backpressure: If writer is slow, main thread blocks
- Memory control: Only one batch in flight at a time
- Prevents OOM on large corpora

**Memory in channel**:
```
Channel State:
├── Sender (Main thread): 8 bytes
├── Receiver (Writer thread): 8 bytes
└── Buffer: 1 slot × BatchData size
    └── BatchData: ~100MB for 1000 documents
```

---

## Key Functions

### `index_corpus_file()` - Entry Point

```
Python Thread (with GIL)
    │
    ▼
py.detach(|| { ... })  ──────▶ Rust Thread (GIL released)
    │
    └── index_corpus_internal()
            │
            ├── Main Loop (sequential batches)
            │       │
            │       └── process_batch() ───▶ Rayon Thread Pool
            │                                    │
            │                                    ├── Thread 0: parse book A
            │                                    ├── Thread 1: parse book B
            │                                    ├── Thread 2: parse book C
            │                                    └── ... (work stealing)
            │
            └── Writer Thread (spawned once)
                    │
                    └── write_segment() (I/O bound)
```

**Context Switch Points**:
1. `py.detach()`: Python → Rust (GIL released)
2. `Rayon par_iter`: Main → Worker threads
3. `tx.send()`: Main → Writer (if channel full, blocks)
4. `rx.recv()`: Writer waits for next batch

---

### `write_segment()` - Core Write Logic

**Memory Flow**:
```
Input: BatchData (heap, ~100MB)
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. collect_chunks()                                             │
│    Flatten ProcessedDoc → parallel arrays                       │
│    ┌────────────────────────────────────────────────────────┐   │
│    │ book_ids: Vec<String>        ~10KB                     │   │
│    │ chunk_to_book: Vec<u16>      ~2 bytes/chunk            │   │
│    │ doc_lengths: Vec<u32>        ~4 bytes/chunk            │   │
│    │ chunk_freq_maps: Vec<...>    ~1KB/chunk                │   │
│    └────────────────────────────────────────────────────────┘   │
│                                                                 │
│ 2. build_inverted_index()                                       │
│    Aggregate term→postings across all chunks                    │
│    ┌────────────────────────────────────────────────────────┐   │
│    │ FxHashMap<String, Vec<(u32, u32)>>                     │   │
│    │ Key: term string                                       │   │
│    │ Value: [(doc_id, freq), ...]                           │   │
│    │ Total size: ~50MB-200MB depending on vocabulary        │   │
│    └────────────────────────────────────────────────────────┘   │
│                                                                 │
│ 3. Sort terms (par_sort_unstable)                               │
│    In-place sorting, no extra allocation                        │
│                                                                 │
│ 4. Parallel encode postings                                     │
│    ┌────────────────────────────────────────────────────────┐   │
│    │ Rayon par_iter():                                      │   │
│    │   encode_postings_separated() for each term            │   │
│    │   Output: (Vec<u8>, Vec<u8>) per term                  │   │
│    └────────────────────────────────────────────────────────┘   │
│                                                                 │
│ 5. Build FST (terms → index)                                    │
│    ┌────────────────────────────────────────────────────────┐   │
│    │ fst::Map::from_iter()                                  │   │
│    │ Streaming construction, ~1.5x input size               │   │
│    └────────────────────────────────────────────────────────┘   │
│                                                                 │
│ 6. Write files (sequential I/O)                                 │
│    ┌────────────────────────────────────────────────────────┐   │
│    │ fs::write() for each file                              │   │
│    │ Files: terms.fst, offsets.bin, postings_*.bin, etc.    │   │
│    └────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## File Format Details

### Offset Table (`offsets.bin`)

Each term has a 28-byte entry:
```
┌────────────────────────────────────────────────────────────┐
│ Bytes 0-7:   doc_offset (u64)  - Offset into postings_docs │
│ Bytes 8-11:  doc_len (u32)     - Length of doc data        │
│ Bytes 12-19: freq_offset (u64) - Offset into postings_freqs│
│ Bytes 20-23: freq_len (u32)    - Length of freq data       │
│ Bytes 24-27: doc_count (u32)   - Number of postings        │
└────────────────────────────────────────────────────────────┘
```

**Why separate doc/freq files?**
- Different access patterns during search
- Doc IDs accessed for all terms
- Frequencies only for scoring (can be prefetched)

---

## Memory Peak Analysis

For a batch of 1000 documents with 1000 chunks each:

| Stage | Peak Memory |
|-------|-------------|
| Raw text | ~500MB |
| After chunking | ~500MB (same data, different structure) |
| After tokenization | ~200MB (arena reused) |
| Inverted index | ~300MB |
| Encoded postings | ~100MB (compressed) |
| **Total peak** | ~1GB |

**Memory drops after**:
- Tokenization: Arena reset frees all tokens
- Segment write: BatchData consumed, freed

---

## Error Handling

```rust
fn write_segment(data: BatchData) -> std::io::Result<SegmentMeta>
```

**Failure modes**:
1. Disk full → `std::io::Error`
2. Permission denied → `std::io::Error`
3. FST construction error → Wrapped as `std::io::Error`

**On error**: Partial segment may exist; caller should clean up.
