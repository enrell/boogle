# reader.rs - Memory-Mapped Segment Reading

## Purpose
Provides zero-copy access to on-disk segment files using memory mapping. This is performance-critical code that directly interfaces with the OS virtual memory system.

## Memory Mapping Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PROCESS ADDRESS SPACE                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Stack (8MB typical)                                                │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ Local variables, function frames                         │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                     │
│  Heap                                                               │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ Rust allocations (Vec, String, HashMap, etc.)            │       │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                     │
│  Memory-Mapped Regions (per segment)                                │
│  ┌───────────────────────────────────────────────────────────┐      │
│  │ terms.fst      │ ~10MB   │ Read-only, shared              │      │
│  ├────────────────┼─────────┼────────────────────────────────┤      │
│  │ offsets.bin    │ ~1MB    │ Read-only, shared              │      │
│  ├────────────────┼─────────┼────────────────────────────────┤      │
│  │ postings_docs  │ ~50MB   │ Read-only, shared              │      │
│  ├────────────────┼─────────┼────────────────────────────────┤      │
│  │ postings_freqs │ ~50MB   │ Read-only, shared              │      │
│  ├────────────────┼─────────┼────────────────────────────────┤      │
│  │ chunks.bin     │ ~5MB    │ Read-only, shared              │      │
│  ├────────────────┼─────────┼────────────────────────────────┤      │
│  │ doc_lengths    │ ~4MB    │ Read-only, shared              │      │
│  └───────────────────────────────────────────────────────────┘      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## SegmentReader Structure

```rust
pub struct SegmentReader {
    pub terms_fst: FstMap<Mmap>,        // FST backed by mmap
    pub offsets_mmap: Mmap,             // Raw mmap handle
    pub postings_docs_mmap: Mmap,       // Raw mmap handle
    pub postings_freqs_mmap: Mmap,      // Raw mmap handle
    pub chunks_mmap: Mmap,              // Raw mmap handle
    pub doc_lengths_mmap: Mmap,         // Raw mmap handle
    pub base_doc_id: u32,               // Stack
    pub num_docs: u32,                  // Stack
}
```

**Memory footprint**: ~200 bytes stack + mmap regions (no heap copy!)

---

## Page Fault Flow

When accessing mmap data:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PAGE FAULT HANDLING                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. Code accesses mmap[offset]                                      │
│     │                                                               │
│     ▼                                                               │
│  2. CPU checks page table                                           │
│     ├── Page present? → Return data (nanoseconds)                   │
│     └── Page not present? → Page fault                              │
│             │                                                       │
│             ▼                                                       │
│  3. OS Kernel handles fault                                         │
│     ├── Allocate physical page                                      │
│     ├── Read from disk (milliseconds!)                              │
│     ├── Update page table                                           │
│     └── Resume process                                              │
│             │                                                       │
│             ▼                                                       │
│  4. Subsequent accesses to same page: fast                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

**Context switch**: Page fault causes kernel mode switch (~1μs overhead + I/O time)

---

## PostingsIter - Streaming Iterator

### Structure
```rust
pub struct PostingsIter<'a> {
    doc_data: &'a [u8],           // Slice into mmap
    freq_data: &'a [u8],          // Slice into mmap
    doc_pos: usize,               // Current position
    freq_pos: usize,              // Current position
    current_doc: u32,             // Accumulated doc ID
    count_left: usize,            // Remaining postings
    doc_buffer: [u32; 128],       // Stack buffer!
    freq_buffer: [u32; 128],      // Stack buffer!
    buffer_idx: usize,            // Position in buffer
    buffer_len: usize,            // Valid entries
    bitpacker: BitPacker4x,       // SIMD decompressor
}
```

**Memory**: 1KB stack (two 512-byte buffers) + references to mmap

### Iteration Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                      POSTINGS ITERATION                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Block-based decompression (128 postings at a time):                │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ mmap region (postings_docs.bin)                             │    │
│  │ ┌─────┬─────────────────┬─────┬─────────────────┬─────┐     │    │
│  │ │bits │ compressed data │bits │ compressed data │ ... │     │    │
│  │ │(1B) │ (bits*16 bytes) │(1B) │ (bits*16 bytes) │     │     │    │
│  │ └─────┴─────────────────┴─────┴─────────────────┴─────┘     │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                        │                                            │
│                        ▼                                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ SIMD Decompress (BitPacker4x)                               │    │
│  │ Input:  128 compressed values                               │    │
│  │ Output: [u32; 128] in doc_buffer (STACK)                    │    │
│  │ Speed:  ~4GB/s on modern CPUs                               │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                        │                                            │
│                        ▼                                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ Iterator returns one (doc_id, freq) at a time               │    │
│  │ doc_id = previous + delta (from buffer)                     │    │
│  │ freq = buffer value (from freq_buffer)                      │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
│  Tail handling (< 128 remaining):                                   │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ VarInt decode one-by-one                                    │    │
│  │ Slower but necessary for partial blocks                     │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Access Patterns

### Term Lookup (FST)
```
get_doc_freq("hello")
    │
    ▼
FST traversal (terms_fst)
    │ Each byte comparison may cause page fault
    │ FST is compact, usually fits in cache
    ▼
Returns: term index (u64)
    │
    ▼
Offset table lookup (offsets_mmap)
    │ Direct indexing: index * 28 bytes
    │ Usually cached after first access
    ▼
Returns: doc_count (u32)
```

### Postings Access
```
get_postings_iter("hello")
    │
    ▼
FST lookup → term index
    │
    ▼
Offset table → (doc_offset, doc_len, freq_offset, freq_len, count)
    │
    ▼
Create PostingsIter with slices into mmap regions
    │ No copy! Just pointer arithmetic
    ▼
Iterator ready
```

---

## Cache Behavior

**Expected cache hit rates**:

| Data | Size | Cache Level | Hit Rate |
|------|------|-------------|----------|
| FST | ~10MB | L3 / RAM | 90%+ |
| Offsets | ~1MB | L3 | 95%+ |
| Postings | ~100MB | RAM / Disk | Varies |
| Doc lengths | ~4MB | L3 / RAM | 80%+ |

**Prefetch opportunities**:
- Sequential scan of postings → OS prefetches ahead
- Random access → Consider `madvise(MADV_RANDOM)`

---

## Thread Safety

`SegmentReader` is `Send + Sync` because:
- `Mmap` is `Send + Sync` (immutable after creation)
- `FstMap<Mmap>` is `Send + Sync`
- All fields are read-only after construction

**Concurrent access**: Multiple threads can read same segment simultaneously, sharing page cache.
