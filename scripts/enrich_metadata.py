#!/usr/bin/env python3
"""
Metadata Enrichment Script
---------------------------
Fetches ratings and popularity data from Open Library for existing books.
"""

import argparse
import sys
import logging
from typing import Optional

sys.path.append(".")

from sqlalchemy import select
from src.db.database import DatabaseManager
from src.db.models import Book
from src.enrichment.openlibrary import OpenLibraryClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def enrich_books(use_sqlite: bool = False, limit: int | None = None, batch_size: int = 50):
    """
    Enrich books with Open Library metadata.
    """
    db = DatabaseManager(use_sqlite=use_sqlite)
    client = OpenLibraryClient()
    
    # Get all books that don't have enrichment data yet
    logger.info("Fetching candidates for enrichment...")
    
    with db.get_session() as session:
        stmt = select(Book.book_id, Book.title, Book.author)\
            .where(Book.ratings_average.is_(None))\
            .order_by(Book.book_id)
            
        if limit:
            stmt = stmt.limit(limit)
            
        # Fetch all candidates into memory to avoid holding DB lock during network calls
        candidates = session.execute(stmt).all()
            
    total = len(candidates)
    logger.info(f"Found {total} books to enrich")
    
    enriched_count = 0
    failed_count = 0
    
    for i, (book_id, title, author) in enumerate(candidates, 1):
        if not title:
            continue
            
        logger.info(f"[{i}/{total}] Enriching '{title}' by {author}")
        
        try:
            enriched = client.enrich_book(title, author or "Unknown")
            
            if enriched:
                # Update database (new session for short transaction)
                with db.get_session() as session:
                    book = session.execute(
                        select(Book).where(Book.source == 'gutenberg', Book.book_id == str(book_id))
                    ).scalar_one_or_none()
                    
                    if book:
                        book.ratings_average = enriched.ratings_average
                        book.ratings_count = enriched.ratings_count
                        book.want_to_read_count = enriched.want_to_read_count
                        book.edition_count = enriched.edition_count
                        # Session commit executes on exit
                
                enriched_count += 1
                disp_rating = f"{enriched.ratings_average:.2f}" if enriched.ratings_average else "N/A"
                logger.info(f"  ✓ Rating: {disp_rating}, Want-to-read: {enriched.want_to_read_count}")
            else:
                failed_count += 1
                logger.debug(f"  ✗ No data found")
                
        except Exception as e:
            failed_count += 1
            logger.error(f"  ✗ Error: {e}")
            
    logger.info(f"\nEnrichment complete!")
    logger.info(f"  Total processed: {total}")
    logger.info(f"  Successfully enriched: {enriched_count}")
    logger.info(f"  Failed: {failed_count}")
    
    db.close()


def main():
    parser = argparse.ArgumentParser(description="Enrich books with Open Library metadata")
    parser.add_argument("--sqlite", action="store_true", help="Use SQLite database")
    parser.add_argument("--limit", type=int, help="Limit number of books to enrich (for testing)")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for progress reporting")
    args = parser.parse_args()
    
    enrich_books(
        use_sqlite=args.sqlite,
        limit=args.limit,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
