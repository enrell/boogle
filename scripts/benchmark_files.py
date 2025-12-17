#!/usr/bin/env python3
"""
Boogle FileSearcher Benchmark
Measures latency of the pure Rust FileSearcher implementation.
"""
import time
import os
import statistics
from rust_bm25 import FileSearcher
from src.indexer.stopwords import load_stopwords

QUERIES = [
    "shakespeare",
    "war",
    "love",
    "declaration of independence",
    "war and peace",
    "history of america",
    "thermodynamics",
    "mesopotamia",
    "the constitution",
    "american revolution",
]

def run_benchmark():
    index_dir = os.getenv("INDEX_DIR", "data/index")
    print(f"Loading index from {index_dir}...")
    
    try:
        searcher = FileSearcher(index_dir)
    except Exception as e:
        print(f"Error loading index: {e}")
        return

    stopwords = list(load_stopwords())
    searcher.set_stopwords(stopwords)
    
    print(f"Index loaded. Docs: {searcher.num_docs}, AvgDL: {searcher.avgdl:.2f}")

    # Warmup
    print("Warming up...")
    for q in QUERIES:
        searcher.search(q, 10)

    print("Running benchmark...")
    latencies = []
    
    for q in QUERIES:
        start = time.perf_counter()
        results = searcher.search(q, 10)
        elapsed = (time.perf_counter() - start) * 1000
        latencies.append(elapsed)
        print(f"{elapsed:6.2f} ms  {q:30} ({len(results)} hits)")

    avg_ms = statistics.mean(latencies)
    print("\n" + "=" * 40)
    print(f"Average Latency: {avg_ms:.2f} ms")
    print(f"QPS:             {1000/avg_ms:.2f}")
    print("=" * 40)

if __name__ == "__main__":
    run_benchmark()
