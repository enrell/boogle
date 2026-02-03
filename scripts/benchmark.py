#!/usr/bin/env python3
"""
Boogle Comprehensive Benchmark Suite
------------------------------------
1. Indexing Performance: Measures throughput (books/sec, MB/sec) of the Rust indexer.
2. Search Performance (Library): Measures raw latency of the finding engine directly via FFI.
3. Search Performance (API): Measures end-to-end latency via HTTP against a running Boogle server.
"""

import argparse
import asyncio
import json
import logging
import os
import shutil
import statistics
import sys
import time
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import List, Dict, Any, Optional
from glob import glob

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("benchmark")

QUERIES = [
    "shakespeare",
    "war",
    "love",
    "declaration of independence",
    "war and peace",
    "history of america",
    "thermodynamics",
    "javascript",
    "machine learning",
    "the constitution",
]

@dataclass
class BenchmarkResult:
    test_type: str
    metrics: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)

class BoogleBenchmark:
    def __init__(self, use_sqlite: bool = False):
        self.use_sqlite = use_sqlite
        self.stopwords = []
        try:
            from src.indexer.stopwords import load_stopwords
            self.stopwords = list(load_stopwords())
        except ImportError:
            logger.warning("Could not load stopwords. Ensure you are in the project root.")

    # --- Indexing Benchmark ---
    
    def run_indexing_benchmark(self, num_books: int = 2000, batch_size: int = 2000) -> BenchmarkResult:
        logger.info(f"Starting Indexing Benchmark (N={num_books})...")
        
        bench_dir = Path("data/bench_temp")
        books_dir = bench_dir / "books"
        index_dir = bench_dir / "index"
        chunks_dir = bench_dir / "chunks"
        
        # Cleanup
        if bench_dir.exists():
            shutil.rmtree(bench_dir)
        for d in [books_dir, index_dir, chunks_dir]:
            d.mkdir(parents=True, exist_ok=True)
            
        # Prepare Data
        src_dir = Path("data/books")
        if not src_dir.exists():
            logger.error(f"Source books directory {src_dir} not found. Run 'boogle seed' first.")
            return BenchmarkResult("indexing", {"error": "no_data"})
            
        all_files = []
        for ext in ["*.epub", "*.txt", "*.pdf"]:
            all_files.extend(glob(str(src_dir / ext)))
        
        if not all_files:
            logger.error("No books found in data/books.")
            return BenchmarkResult("indexing", {"error": "no_books"})
            
        # Limit files
        files_to_index = sorted(all_files)[:num_books]
        logger.info(f"Staging {len(files_to_index)} books...")
        
        for src in files_to_index:
            src_path = Path(src)
            try:
                (books_dir / src_path.name).symlink_to(src_path.absolute())
            except FileExistsError:
                pass
                
        # Run Indexer
        from rust_bm25 import index_corpus_file
        
        logger.info("Running indexer...")
        start_time = time.perf_counter()
        
        indexed_count, chunks_count = index_corpus_file(
            str(books_dir),
            str(index_dir),
            str(chunks_dir),
            self.stopwords,
            chunk_size=1000,
            chunk_overlap=100,
            batch_size=batch_size,
        )
        
        duration = time.perf_counter() - start_time
        
        # Calculate size
        index_size_mb = sum(f.stat().st_size for f in index_dir.rglob("*") if f.is_file()) / (1024 * 1024)
        
        metrics = {
            "duration_seconds": round(duration, 3),
            "books_indexed": indexed_count,
            "chunks_generated": chunks_count,
            "books_per_second": round(indexed_count / duration, 1),
            "chunks_per_second": round(chunks_count / duration, 1),
            "index_size_mb": round(index_size_mb, 2)
        }
        
        logger.info(f"Indexing Complete: {json.dumps(metrics, indent=2)}")
        
        # Cleanup
        shutil.rmtree(bench_dir)
        return BenchmarkResult("indexing", metrics)

    # --- Library Search Benchmark ---

    def run_library_search_benchmark(self, iterations: int = 5, warmup: int = 2) -> BenchmarkResult:
        logger.info("Starting Library Search Benchmark...")
        from rust_bm25 import FileSearcher
        from src.db.database import PostgresRepository
        
        index_dir = os.getenv("INDEX_DIR", "data/index")
        if not Path(index_dir).exists():
             logger.error(f"Index directory {index_dir} not found. Run 'boogle index' first.")
             return BenchmarkResult("library_search", {"error": "no_index"})

        logger.info("Loading searcher and connecting to DB...")
        searcher = FileSearcher(index_dir)
        searcher.set_stopwords(self.stopwords)
        
        try:
            db = PostgresRepository(use_sqlite=self.use_sqlite)
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return BenchmarkResult("library_search", {"error": str(e)})

        # Warmup
        for _ in range(warmup):
            for q in QUERIES:
                searcher.search(q, 10)
        
        latencies = []
        for _ in range(iterations):
            for q in QUERIES:
                start = time.perf_counter()
                # Simulate full integration flow
                results = searcher.search(q, 50)
                # Simulate DB lookups (batched or single, similar to API)
                for bid, _, _ in results[:10]:
                    _ = db.get_book("gutenberg", bid)
                latencies.append((time.perf_counter() - start) * 1000)
                
        metrics = self._calculate_metrics(latencies)
        logger.info(f"Library Search Result: {json.dumps(metrics, indent=2)}")
        return BenchmarkResult("library_search", metrics)

    # --- API Search Benchmark ---

    async def run_api_benchmark(self, url: str, concurrency: int = 1, iterations: int = 50) -> BenchmarkResult:
        logger.info(f"Starting API Benchmark against {url} (Concurrency={concurrency})...")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Health check
            try:
                resp = await client.get(f"{url}/health")
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"API is not reachable at {url}: {e}")
                return BenchmarkResult("api_search", {"error": "api_unreachable"})

            latencies = []
            errors = 0
            
            queue = asyncio.Queue()
            for _ in range(iterations):
                for q in QUERIES:
                    queue.put_nowait(q)
            
            async def worker():
                nonlocal errors
                while not queue.empty():
                    try:
                        q = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                        
                    start = time.perf_counter()
                    try:
                        resp = await client.get(f"{url}/search", params={"query": q, "limit": 10})
                        resp.raise_for_status()
                        duration = (time.perf_counter() - start) * 1000
                        latencies.append(duration)
                    except Exception as e:
                        logger.warning(f"Request failed: {e}")
                        errors += 1
                    finally:
                        queue.task_done()
            
            tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
            await asyncio.gather(*tasks)
            
            metrics = self._calculate_metrics(latencies)
            metrics["errors"] = errors
            metrics["total_requests"] = len(latencies) + errors
            
            logger.info(f"API Search Result: {json.dumps(metrics, indent=2)}")
            return BenchmarkResult("api_search", metrics)

    def _calculate_metrics(self, latencies: List[float]) -> Dict[str, float]:
        if not latencies:
            return {}
        return {
            "p50_ms": round(statistics.median(latencies), 2),
            "p90_ms": round(statistics.quantiles(latencies, n=10)[8], 2) if len(latencies) >= 10 else 0,
            "p99_ms": round(statistics.quantiles(latencies, n=100)[98], 2) if len(latencies) >= 100 else 0,
            "mean_ms": round(statistics.mean(latencies), 2),
            "min_ms": round(min(latencies), 2),
            "max_ms": round(max(latencies), 2),
            "qps": round(1000 / statistics.mean(latencies), 1) if abs(statistics.mean(latencies)) > 0.001 else 0
        }

async def main():
    parser = argparse.ArgumentParser(description="Boogle Benchmark Tool")
    subparsers = parser.add_subparsers(dest="mode", required=True)
    
    # Indexing Args
    idx_parser = subparsers.add_parser("indexing", help="Benchmark indexing throughput")
    idx_parser.add_argument("--books", type=int, default=2000, help="Number of books to use")
    
    # Library Args
    lib_parser = subparsers.add_parser("library", help="Benchmark internal library components")
    lib_parser.add_argument("--sqlite", action="store_true", help="Use SQLite backend")
    
    # API Args
    api_parser = subparsers.add_parser("api", help="Benchmark HTTP API")
    api_parser.add_argument("--url", default="http://127.0.0.1:8000", help="API Base URL")
    api_parser.add_argument("--concurrency", "-c", type=int, default=5, help="Concurrent clients")
    api_parser.add_argument("--iterations", "-n", type=int, default=10, help="Iterations per query")
    
    # Full suite
    full_parser = subparsers.add_parser("all", help="Run all benchmarks (requires running API)")
    full_parser.add_argument("--sqlite", action="store_true", help="Use SQLite for library tests")
    full_parser.add_argument("--url", default="http://127.0.0.1:8000")

    args = parser.parse_args()
    
    bench = BoogleBenchmark(use_sqlite=getattr(args, "sqlite", False))
    results = []
    
    if args.mode in ["indexing", "all"]:
        results.append(bench.run_indexing_benchmark(num_books=getattr(args, "books", 1000)))
        
    if args.mode in ["library", "all"]:
        results.append(bench.run_library_search_benchmark(iterations=10))
        
    if args.mode in ["api", "all"]:
        concurrency = getattr(args, "concurrency", 5)
        iterations = getattr(args, "iterations", 10)
        results.append(await bench.run_api_benchmark(url=args.url, concurrency=concurrency, iterations=iterations))
        
    # Save Report
    report_file = "benchmark_report.json"
    with open(report_file, "w") as f:
        data = [asdict(r) for r in results]
        json.dump(data, f, indent=2)
        
    print(f"\n✅ Benchmark Suite Completed. Report saved to {report_file}")
    
if __name__ == "__main__":
    asyncio.run(main())
