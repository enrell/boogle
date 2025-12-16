#!/usr/bin/env python3
"""
Boogle Search Performance Benchmark
Measures latency, throughput, and index statistics.
"""
import statistics
import time
import tracemalloc
from dataclasses import dataclass

from src.indexer.ranker import Ranker
from src.indexer.storage import IndexStorage


QUERIES = [
    # Single term
    "shakespeare",
    "war",
    "love",
    # Multi-term
    "declaration of independence",
    "war and peace",
    "history of america",
    # Rare terms
    "thermodynamics",
    "mesopotamia",
    # Common + rare
    "the constitution",
    "american revolution",
]


@dataclass
class QueryResult:
    query: str
    latency_ms: float
    num_results: int


@dataclass 
class BenchmarkReport:
    num_queries: int
    iterations: int
    latencies_ms: list[float]
    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    avg_ms: float
    qps: float
    peak_memory_mb: float
    index_stats: dict


def measure_query(ranker: Ranker, query: str, top_k: int = 10) -> QueryResult:
    start = time.perf_counter()
    results = ranker.search(query, top_k)
    elapsed = (time.perf_counter() - start) * 1000
    return QueryResult(query=query, latency_ms=elapsed, num_results=len(results))


def get_index_stats(storage: IndexStorage) -> dict:
    stats = {}
    
    with storage.pool.connection() as conn:
        # Term count
        row = conn.execute("SELECT COUNT(*) as cnt FROM idx_terms").fetchone()
        stats["num_terms"] = row["cnt"]
        
        # Chunk count
        row = conn.execute("SELECT COUNT(*) as cnt FROM idx_chunks").fetchone()
        stats["num_chunks"] = row["cnt"]
        
        # Postings size
        row = conn.execute("SELECT SUM(LENGTH(postings)) as total FROM idx_terms").fetchone()
        stats["postings_bytes"] = row["total"] or 0
        
        # Top 10 terms by df
        rows = conn.execute("""
            SELECT term, df FROM idx_terms ORDER BY df DESC LIMIT 10
        """).fetchall()
        stats["top_terms_by_df"] = [(r["term"], r["df"]) for r in rows]
        
        # Avg postings size
        row = conn.execute("SELECT AVG(LENGTH(postings)) as avg FROM idx_terms").fetchone()
        stats["avg_postings_bytes"] = round(row["avg"] or 0, 1)
        
        # Table sizes
        rows = conn.execute("""
            SELECT relname, pg_total_relation_size(relid) as size
            FROM pg_catalog.pg_statio_user_tables
            WHERE relname LIKE 'idx_%'
        """).fetchall()
        stats["table_sizes"] = {r["relname"]: r["size"] for r in rows}
    
    # Globals
    stats["num_docs"] = int(storage.get_global("num_docs") or 0)
    stats["avgdl"] = float(storage.get_global("avgdl") or 0)
    
    return stats


def run_benchmark(iterations: int = 3, warmup: int = 1) -> BenchmarkReport:
    print("Initializing...")
    tracemalloc.start()
    
    storage = IndexStorage()
    ranker = Ranker(storage)
    
    # Warmup
    print(f"Warmup ({warmup} iterations)...")
    for _ in range(warmup):
        for q in QUERIES:
            ranker.search(q, 10)
    
    # Benchmark
    print(f"Running benchmark ({iterations} iterations)...")
    all_latencies = []
    query_results: dict[str, list[float]] = {q: [] for q in QUERIES}
    
    for i in range(iterations):
        for q in QUERIES:
            result = measure_query(ranker, q)
            all_latencies.append(result.latency_ms)
            query_results[q].append(result.latency_ms)
    
    # Memory
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    # Stats
    sorted_latencies = sorted(all_latencies)
    n = len(sorted_latencies)
    
    report = BenchmarkReport(
        num_queries=len(QUERIES),
        iterations=iterations,
        latencies_ms=sorted_latencies,
        p50_ms=sorted_latencies[int(n * 0.50)],
        p95_ms=sorted_latencies[int(n * 0.95)],
        p99_ms=sorted_latencies[int(n * 0.99)] if n >= 100 else sorted_latencies[-1],
        min_ms=min(sorted_latencies),
        max_ms=max(sorted_latencies),
        avg_ms=statistics.mean(sorted_latencies),
        qps=1000 / statistics.mean(sorted_latencies),
        peak_memory_mb=peak / 1024 / 1024,
        index_stats=get_index_stats(storage),
    )
    
    cache_stats = storage.cache_stats()
    storage.close()
    
    return report, query_results, cache_stats


def print_report(report: BenchmarkReport, query_results: dict[str, list[float]], cache_stats: dict = None):
    print("\n" + "=" * 60)
    print("BOOGLE SEARCH BENCHMARK REPORT")
    print("=" * 60)
    
    print("\n## Latency Summary")
    print(f"  Queries: {report.num_queries} × {report.iterations} iterations = {report.num_queries * report.iterations} total")
    print(f"  Min:     {report.min_ms:.2f} ms")
    print(f"  Avg:     {report.avg_ms:.2f} ms")
    print(f"  P50:     {report.p50_ms:.2f} ms")
    print(f"  P95:     {report.p95_ms:.2f} ms")
    print(f"  P99:     {report.p99_ms:.2f} ms")
    print(f"  Max:     {report.max_ms:.2f} ms")
    print(f"  QPS:     {report.qps:.1f} queries/sec")
    
    print("\n## Per-Query Latency (avg ms)")
    for q, latencies in sorted(query_results.items(), key=lambda x: statistics.mean(x[1]), reverse=True):
        avg = statistics.mean(latencies)
        print(f"  {avg:6.1f} ms  {q}")
    
    print("\n## Memory")
    print(f"  Peak: {report.peak_memory_mb:.1f} MB")
    
    if cache_stats:
        print("\n## Chunk Cache")
        print(f"  Hits:   {cache_stats['hits']}")
        print(f"  Misses: {cache_stats['misses']}")
        hit_rate = cache_stats['hits'] / max(cache_stats['hits'] + cache_stats['misses'], 1) * 100
        print(f"  Rate:   {hit_rate:.1f}%")
        print(f"  Size:   {cache_stats['size']}/{cache_stats['maxsize']} books")
    
    print("\n## Index Statistics")
    stats = report.index_stats
    print(f"  Documents (chunks): {stats['num_chunks']:,}")
    print(f"  Unique terms:       {stats['num_terms']:,}")
    print(f"  Avg doc length:     {stats['avgdl']:.1f} tokens")
    print(f"  Postings total:     {stats['postings_bytes'] / 1024 / 1024:.1f} MB")
    print(f"  Avg postings/term:  {stats['avg_postings_bytes']:.0f} bytes")
    
    print("\n## Table Sizes")
    for table, size in sorted(stats["table_sizes"].items()):
        print(f"  {table}: {size / 1024:.1f} KB")
    
    print("\n## Top Terms by Document Frequency")
    for term, df in stats["top_terms_by_df"]:
        print(f"  {df:6,} docs  {term}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", "-n", type=int, default=5)
    parser.add_argument("--warmup", "-w", type=int, default=2)
    args = parser.parse_args()
    
    report, query_results, cache_stats = run_benchmark(args.iterations, args.warmup)
    print_report(report, query_results, cache_stats)
