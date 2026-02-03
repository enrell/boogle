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
    
    try:
        from src.enrichment.service import enrich_books_service
        enrich_books_service(db, client, limit, batch_size)
    finally:
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
