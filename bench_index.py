#!/usr/bin/env python3
"""
Benchmark script to analyze indexing performance.
Indexes 2 segments (2000 books) with detailed timing.
"""

import os
import time
import shutil
from pathlib import Path
from glob import glob

def main():
    bench_books_dir = Path("data/bench_books")
    index_dir = Path("data/bench_index")
    chunks_dir = Path("data/bench_chunks")
    
    for d in [bench_books_dir, index_dir, chunks_dir]:
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("BOOGLE INDEXING BENCHMARK")
    print("=" * 60)
    
    books_dir = Path("data/books")
    all_files = sorted(glob(str(books_dir / "*.epub")))[:2000]
    
    print(f"Symlinking {len(all_files)} files to {bench_books_dir}...")
    for src in all_files:
        src_path = Path(src)
        dst = bench_books_dir / src_path.name
        dst.symlink_to(src_path.absolute())
    
    from src.indexer.stopwords import load_stopwords
    stopwords = list(load_stopwords())
    print(f"Loaded {len(stopwords)} stopwords")
    
    from rust_bm25 import index_corpus_file
    
    batch_size = 2000
    
    print(f"\nIndexing {len(all_files)} books in batches of {batch_size}...")
    print(f"Index dir: {index_dir}")
    print()
    
    start = time.perf_counter()
    
    indexed, total_chunks = index_corpus_file(
        str(bench_books_dir),
        str(index_dir),
        str(chunks_dir),
        stopwords,
        chunk_size=1000,
        chunk_overlap=100,
        batch_size=batch_size,
    )
    
    elapsed = time.perf_counter() - start
    
    print()
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Indexed: {indexed} books")
    print(f"Chunks: {total_chunks}")
    print(f"Time: {elapsed:.2f}s")
    print(f"Books/sec: {indexed / elapsed:.1f}")
    print(f"Chunks/sec: {total_chunks / elapsed:.1f}")
    
    for seg_dir in sorted(index_dir.glob("segment_*")):
        total_size = sum(f.stat().st_size for f in seg_dir.glob("*"))
        print(f"{seg_dir.name}: {total_size / 1024 / 1024:.1f} MB")
    
    print()
    print("Cleanup: rm -rf data/bench_*")

if __name__ == "__main__":
    main()

