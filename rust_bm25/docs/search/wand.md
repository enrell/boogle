# wand.rs - WAND Algorithm Implementation

## Purpose
Efficient top-k retrieval using WAND (Weak AND) algorithm. Skips scoring documents that can't make top-k.

## Algorithm Overview

WAND maintains a threshold and skips documents whose upper-bound score is below it.

```
Traditional: Score ALL matching documents, then sort
WAND:        Skip documents that can't beat current top-k threshold

Complexity: O(k * avg_postings) vs O(total_postings)
```

## Data Structures

```rust
struct TermInfo {
    idf: f32,                           // Pre-computed IDF
    upper_bound: f32,                   // Max possible contribution
    postings: FxHashMap<u32, u32>,      // doc_id → freq
}

struct ScoredDoc {
    doc_id: u32,
    score: f32,
}

pub struct WandSearcher {
    k1: f32,
    b: f32,
    num_docs: u32,
    avgdl: f32,
    stopwords: FxHashSet<String>,
}
```

## Memory Layout

```
WandSearcher (stack)
├── k1, b, num_docs, avgdl (16 bytes)
└── stopwords (heap, FxHashSet)

During search:
├── terms: Vec<TermInfo>
│   └── Each TermInfo has:
│       └── postings: FxHashMap (DECODED, in heap!)
├── candidates: Vec<(f32, u32)>  // (upper_bound, doc_id)
├── doc_lengths: FxHashMap<u32, u32>
└── heap: BinaryHeap<ScoredDoc> (min-heap for top-k)
```

**Note**: WAND decodes all postings to HashMap upfront (memory intensive)

## Search Flow

```
search(query, posting_data, top_k)
    │
    ├── 1. build_term_info()
    │       For each (df, encoded_postings):
    │           decode_postings_internal() → Vec<(u32, u32)>
    │           Convert to HashMap for O(1) lookup
    │           Compute IDF, upper_bound
    │
    ├── 2. Sort terms by posting count (ascending)
    │       Short lists first for early termination
    │
    ├── 3. compute_candidates()
    │       Intersect posting lists progressively
    │       Keep candidates appearing in most lists
    │
    ├── 4. compute_upper_bounds()
    │       For each candidate: sum of term upper_bounds
    │
    ├── 5. Sort candidates by upper_bound (descending)
    │
    └── 6. wand_score()
            For each candidate (high upper_bound first):
                if upper_bound <= threshold: EARLY STOP
                Compute actual score
                Update heap if better than min
```

## Upper Bound Computation

```rust
upper_bound = idf * (k1 + 1.0)
```

This is the max BM25 score when:
- tf = infinity
- doc_len = 0 (maximizes score)

## Candidate Pruning

```
┌─────────────────────────────────────────────────────────────────┐
│                    CANDIDATE SELECTION                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Query: "quick brown fox"                                       │
│                                                                  │
│  Term "the":    [doc1, doc2, doc3, ..., doc10000]  (common)    │
│  Term "quick":  [doc5, doc100, doc500]             (rare)       │
│  Term "fox":    [doc5, doc200, doc500, doc800]     (medium)     │
│                                                                  │
│  Strategy:                                                       │
│  1. Start with smallest list (quick): {5, 100, 500}            │
│  2. Intersect with next: {5, 500} ∩ fox                         │
│  3. If intersection >= top_k * 2: use it                        │
│  4. Otherwise: keep union                                       │
│                                                                  │
│  Result: Candidates = {5, 500} (high recall, low count)         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## WAND Loop

```rust
for (upper, doc_id) in candidates {
    // Early termination
    if heap.len() >= top_k && upper <= threshold {
        break;  // No remaining candidates can beat threshold
    }
    
    // Full scoring (expensive)
    let score = terms.iter()
        .filter_map(|t| t.postings.get(&doc_id))
        .map(|&tf| bm25_score(tf, doc_len, t.idf))
        .sum();
    
    // Heap update
    if heap.len() < top_k || score > threshold {
        heap.push(ScoredDoc { doc_id, score });
        if heap.len() > top_k { heap.pop(); }
        threshold = heap.peek().unwrap().score;
    }
}
```

## Memory vs CPU Tradeoff

| Approach | Memory | CPU |
|----------|--------|-----|
| Decode all postings | High | Low (HashMap lookups) |
| Stream postings | Low | High (decode per access) |

Current implementation: Decode all upfront (memory-intensive but fast)

## When to Use WAND

✓ Use when:
- Large posting lists (>10K postings)
- Small top_k relative to corpus
- Query has both common and rare terms

✗ Avoid when:
- All terms are rare
- top_k is large (approaching corpus size)
- Memory constrained
