# realtime.rs - Near Real-Time Indexer

## Purpose
Hybrid indexer combining disk segments with RAM buffer for NRT search. Implements LSM-tree-like architecture.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    RealTimeIndexer                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐   ┌──────────────────┐   ┌─────────────┐  │
│  │  disk_index      │   │  memory_index    │   │  wal        │  │
│  │  (FileSearcher)  │   │  (RamIndex)      │   │  (Wal)      │  │
│  │  [Arc<RwLock>]   │   │  [Arc<RwLock>]   │   │ [Arc<Mutex>]│  │
│  └──────────────────┘   └──────────────────┘   └─────────────┘  │
│         │                       │                     │         │
│         │   Search Path         │  Write Path         │         │
│         ▼                       ▼                     ▼         │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Segment Files                         │   │
│  │  (immutable, mmap)                                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Synchronization

```rust
pub struct RealTimeIndexer {
    index_dir: String,                        // Immutable
    disk_index: Arc<RwLock<FileSearcher>>,    // Many readers OR one writer
    memory_index: Arc<RwLock<RamIndex>>,      // Many readers OR one writer
    wal: Arc<Mutex<Wal>>,                     // One accessor at a time
}
```

**Lock ordering**: Always acquire in order: disk → memory → wal

## Write Path

```
add_document(content, metadata)
    │
    ├── 1. memory_index.write().unwrap()
    │       Acquire exclusive lock on RAM index
    │       Insert document (tokenize, update postings)
    │
    ├── 2. Create Document for WAL
    │       Clone content/metadata
    │
    └── 3. wal.lock().unwrap().append()
            Acquire WAL mutex
            Write NDJSON line
            flush() to OS buffer
```

**Durability**: Document visible immediately, persisted after flush()

## Read Path (Search)

```
search(query, top_k)
    │
    ├── 1. Acquire read locks (parallel safe)
    │       disk = disk_index.read()
    │       mem = memory_index.read()
    │
    ├── 2. Federated search (parallel)
    │       rayon::join(
    │           || disk.search(&query, top_k),
    │           || mem.search(&query)
    │       )
    │
    ├── 3. Merge results
    │       Combine disk + memory results
    │       Convert mem doc_ids to book_ids
    │
    └── 4. Sort by score, truncate to top_k
```

## Recovery Flow (Constructor)

```
new(index_dir)
    │
    ├── 1. Open WAL file
    │       Wal::open(path.join("index.wal"))
    │
    ├── 2. Read all WAL entries
    │       wal.read_all() → Vec<Document>
    │
    ├── 3. Load disk index
    │       FileSearcher::new(&index_dir)
    │       next_id = disk.num_docs()
    │
    ├── 4. Initialize RAM with correct base ID
    │       RamIndex::new(next_id)
    │
    └── 5. Replay WAL
            for doc in recovered_docs:
                ram.insert(doc.content, doc.metadata)
```

**Crash recovery**: All unflushed documents restored from WAL

## Flush Operation

```
flush()
    │
    ├── 1. Acquire memory write lock
    │       Blocks new inserts
    │
    ├── 2. Count documents
    │       count = mem.docs.len()
    │
    ├── 3. Clear memory index
    │       mem.clear()
    │
    └── 4. Truncate WAL
            wal.truncate()
            (TODO: actually write to disk segment)
```

**Current limitation**: Flush clears memory but doesn't persist to disk segment.

## Thread Safety Analysis

| Operation | Locks Held | Blocking |
|-----------|------------|----------|
| add_document | memory(W), wal(X) | Yes |
| search | disk(R), memory(R) | No (parallel) |
| flush | memory(W), wal(X) | Yes |

**Concurrency**:
- Multiple searches can run in parallel
- One write blocks other writes
- Write blocks searches on memory (briefly)
