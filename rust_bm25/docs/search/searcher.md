# searcher.rs - File-Based BM25 Search

## Purpose
Main search interface over disk-based segments. Handles multi-segment search, fuzzy matching, and result aggregation.

## Structure

```rust
pub struct FileSearcher {
    segments: Vec<SegmentReader>,     // Multiple segment readers
    total_docs: u32,                   // Global doc count
    avgdl: f32,                        // Pre-computed average doc length
    stopwords: FxHashSet<String>,      // Query-time filtering
}
```

## Memory Layout

```
FileSearcher (on stack/heap)
├── segments: Vec<SegmentReader>
│   └── Each SegmentReader has:
│       ├── terms_fst (mmap)
│       ├── offsets_mmap (mmap)
│       └── ... (all mmap, no heap copy)
├── total_docs: u32
├── avgdl: f32
└── stopwords: FxHashSet<String> (heap)
```

**Total heap**: ~1KB + segment mmaps (shared with OS)

## Search Flow

```
search(query, top_k)
    │
    ├── 1. Tokenize query
    │       analyze(query) → Vec<String>
    │       Filter stopwords
    │
    ├── 2. For each token: score_token()
    │       │
    │       ├── resolve_term() - exact or fuzzy
    │       │   └── Returns (terms, total_df)
    │       │
    │       └── For each resolved term:
    │           For each segment:
    │               get_postings_iter()
    │               For each (doc_id, tf):
    │                   score += bm25_score()
    │
    └── 3. select_top_k()
            QuickSelect + Sort
```

## Term Resolution

```
resolve_term("helo")  // typo
    │
    ├── 1. Try exact match
    │       segment.get_doc_freq("helo") → None
    │
    ├── 2. Fuzzy search
    │       dist = len > 4 ? 2 : 1
    │       segment.get_fuzzy_terms("helo", 2)
    │       └── Uses Levenshtein automaton on FST
    │
    └── 3. Return candidates
            ["hello", "help", "held"] with total_df

Memory: O(candidates) temporary strings
```

## BM25 Scoring

```rust
const K1: f32 = 1.5;
const B: f32 = 0.75;

fn compute_idf(&self, df: u32) -> f32 {
    let n = self.total_docs as f32;
    let df = df as f32;
    ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
}

fn bm25_score(&self, tf: f32, doc_len: f32, idf: f32) -> f32 {
    let numerator = tf * (K1 + 1.0);
    let denominator = tf + K1 * (1.0 - B + B * doc_len / self.avgdl);
    idf * numerator / denominator
}
```

## Top-K Selection

```rust
fn select_top_k(doc_scores, top_k) {
    // 1. Convert HashMap to Vec
    let mut results: Vec<_> = doc_scores.into_iter().collect();
    
    // 2. QuickSelect to partition
    results.select_nth_unstable_by(k - 1, |a, b| ...);
    
    // 3. Truncate and sort top k
    results.truncate(k);
    results.sort_unstable_by(...);
}
```

**Complexity**: O(n) average for selection, O(k log k) for final sort

## Multi-Segment Search

```
┌─────────────────────────────────────────────────────────────────┐
│                    MULTI-SEGMENT SEARCH                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Query: "hello world"                                           │
│                                                                  │
│  For token "hello":                                             │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Segment 0        │ Segment 1        │ Segment 2          │  │
│  │ df=100           │ df=50            │ df=75              │  │
│  │ total_df = 225   │                  │                    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  IDF computed from total_df across all segments                 │
│                                                                  │
│  Postings iterated from each segment:                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Seg0: (doc0, 2), (doc5, 1), ...                          │  │
│  │ Seg1: (doc1000, 3), (doc1002, 1), ...                    │  │
│  │ Seg2: (doc2000, 1), ...                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  All scores accumulated in single FxHashMap<u32, f32>           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Page Fault Pattern

During search:
1. FST lookup: ~1-2 page faults (usually cached)
2. Offset read: 1 page fault per term
3. Postings read: O(postings/4096) page faults
4. Doc lengths: Scattered access, many page faults

**Optimization**: Sequential postings access benefits from OS read-ahead
