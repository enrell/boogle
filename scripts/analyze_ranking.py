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

    # Search returns (book_id, score, chunk_id)
    # We fetch more than top_k to handle potential deduping if chunks are close
    # But for now assuming the searcher handles standard retrieval
    raw_results = searcher.search(query, top_k * 5)
    
    # Deduplicate by book_id, keeping highest score
    seen_books = set()
    results = []
    
    for book_id, score, _ in raw_results:
        if book_id in seen_books:
            continue
        seen_books.add(book_id)
        results.append((book_id, score))
        if len(results) >= top_k:
            break
            
    if not results:
        print("   (No results found)")
        return

    for i, (book_id, score) in enumerate(results, 1):
        meta = db.get_book("gutenberg", book_id)
        if meta:
            title = meta.get("title", "Unknown")[:40] # Truncate for display
            author = meta.get("author", "Unknown")[:20]
        else:
            title = "[Metadata Missing]"
            author = "Unknown"
            
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
