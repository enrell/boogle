# wal.rs - Write-Ahead Log

## Purpose
Durability layer for RealTimeIndexer. Ensures documents survive crashes before being flushed to segments.

## Structure

```rust
pub struct Wal {
    path: PathBuf,              // 24 bytes + path_len
    writer: BufWriter<File>,    // Buffered file handle
}
```

## File Format

Newline-delimited JSON (NDJSON):
```
{"id":0,"content":"First doc...","metadata":"{}","length":100}
{"id":1,"content":"Second doc...","metadata":"{}","length":150}
```

**Why NDJSON?**
- Human readable for debugging
- Easy to recover partial writes (skip invalid lines)
- Serde support built-in

## Memory Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         WAL WRITE PATH                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Document (Rust heap)                                           │
│       │                                                         │
│       ▼                                                         │
│  serde_json::to_string()                                        │
│       │ Allocates: ~2x document size                            │
│       ▼                                                         │
│  BufWriter (8KB default buffer)                                 │
│       │ Copies into buffer                                      │
│       ▼                                                         │
│  flush()                                                        │
│       │ System call: write() to kernel                          │
│       ▼                                                         │
│  OS Page Cache                                                  │
│       │ Async write to disk (unless fsync)                      │
│       ▼                                                         │
│  Disk                                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Operations

### append(doc)
```rust
pub fn append(&mut self, doc: &Document) -> std::io::Result<()> {
    let serialized = serde_json::to_string(doc)?; // Heap alloc
    writeln!(self.writer, "{}", serialized)?;     // Buffer
    self.writer.flush()                            // To OS cache
}
```

**Durability level**: OS page cache (survives process crash, not power loss)

### read_all()
```rust
pub fn read_all(&self) -> std::io::Result<Vec<Document>> {
    let file = File::open(&self.path)?;
    let reader = BufReader::new(file);
    
    Ok(reader.lines()
        .filter_map(|line| line.ok())
        .filter(|line| !line.trim().is_empty())
        .filter_map(|line| serde_json::from_str(&line).ok())
        .collect())
}
```

**Error handling**: Silently skips malformed lines (crash-tolerant)

### truncate()
```rust
pub fn truncate(&mut self) -> std::io::Result<()> {
    self.writer.flush()?;
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&self.path)?;
    self.writer = BufWriter::new(file);
    Ok(())
}
```

**Effect**: Clears WAL after successful flush to segment

## Durability Guarantees

| Failure Mode | Data Loss |
|--------------|-----------|
| Process crash | None (OS cache) |
| OS crash | Since last fsync |
| Power loss | Since last fsync |

**To improve**: Add `fsync()` after flush for power-loss safety (slower)
