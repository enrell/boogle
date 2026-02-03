#!/usr/bin/env python3
"""
Ranking Analysis Tool
---------------------
Performs qualitative analysis of the search engine's ranking quality.
Executes a set of diverse queries and displays the Top-K results with
metadata and scores to allow human evaluation of relevance.
"""

import argparse
import sys
import logging
from typing import List
from dataclasses import dataclass

# Ensure src module is in path
sys.path.append(".")

from rust_bm25 import FileSearcher
from src.db.database import PostgresRepository
from src.indexer.stopwords import load_stopwords

# Configure logging
logging.basicConfig(level=logging.ERROR)

@dataclass
class SearchResult:
    rank: int
    score: float
    title: str
    author: str
    book_id: str

def analyze_query(
    query: str, 
    searcher: FileSearcher, 
    db: PostgresRepository, 
    top_k: int = 5
) -> None:
    print(f"\n🔍 Query: '{query}'")
    print("-" * 80)
    print(f"{'Rank':<5} | {'Score':<8} | {'Book ID':<8} | {'Author':<20} | {'Title'}")
    print("-" * 80)

    raw_results = searcher.search(query, top_k * 20)
    
    # Apply same logic as API: metadata fetch + boost + dedupe
    candidate_ids = {r[0] for r in raw_results}
    candidates_meta = {}
    
    for bid in candidate_ids:
        meta = db.get_book("gutenberg", bid)
        if meta:
            candidates_meta[bid] = meta
    
    unique_books = {}
    query_norm = query.lower()
    query_tokens = set(query_norm.split())
    
    for book_id, base_score, chunk_id in raw_results:
        meta = candidates_meta.get(book_id)
        if not meta:
            continue
            
        title = meta.get("title") or "Unknown"
        author = meta.get("author") or "Unknown"
        
        # Aggressive normalization (same as API)
        title_norm = " ".join("".join(c for c in title.lower() if c.isalnum() or c.isspace()).split())
        author_norm = " ".join("".join(c for c in author.lower() if c.isalnum() or c.isspace()).split())
        dedupe_key = (title_norm, author_norm)
        
        final_score = base_score
        
        # Boost 1: Title Match
        if query_norm in title.lower():
            final_score *= 1.5
            
        # Boost 2: Author Match
        author_tokens = set(author_norm.split())
        if query_tokens & author_tokens:
            final_score *= 2.0
             
        if dedupe_key not in unique_books or final_score > unique_books[dedupe_key][0]:
            unique_books[dedupe_key] = (final_score, book_id)
            
    sorted_unique = sorted(unique_books.values(), key=lambda x: x[0], reverse=True)[:top_k]
            
    if not sorted_unique:
        print("   (No results found)")
        return

    for i, (score, book_id) in enumerate(sorted_unique, 1):
        meta = candidates_meta[book_id]
        title = meta.get("title", "Unknown")[:40]
        author = meta.get("author", "Unknown")[:20]
        print(f"{i:<5} | {score:<8.4f} | {book_id:<8} | {author:<20} | {title}")


def main():
    parser = argparse.ArgumentParser(description="Boogle Ranking Analyzer")
    parser.add_argument("--sqlite", action="store_true", help="Use SQLite database")
    parser.add_argument("--index-dir", default="data/index", help="Path to index directory")
    parser.add_argument("--query", "-q", help="Specific query to run (optional)")
    args = parser.parse_args()

    # Default test suite if no specific query provided
    TEST_QUERIES = [
        "shakespeare",
        "civil war",
        "alice wonderland",
        "philosophy of logic",
        "french revolution",
        "cooking recipes",
        "sherlock holmes"
    ]

    print("🚀 Initializing Search Engine...")
    stopwords = list(load_stopwords())
    searcher = FileSearcher(args.index_dir)
    searcher.set_stopwords(stopwords)
    
    print(f"🔌 Connecting to Database (SQLite={args.sqlite})...")
    db = PostgresRepository(use_sqlite=args.sqlite)

    queries_to_run = [args.query] if args.query else TEST_QUERIES

    try:
        for q in queries_to_run:
            analyze_query(q, searcher, db)
        print("\n" + "="*80)
    except KeyboardInterrupt:
        print("\nAnalysis interrupted.")
    finally:
        db.close()

if __name__ == "__main__":
    main()
