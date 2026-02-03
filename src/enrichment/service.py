"""
Service module for metadata enrichment.
"""
import logging
from typing import Optional
from sqlalchemy import select
from src.db.database import DatabaseManager
from src.db.models import Book
from src.enrichment.openlibrary import OpenLibraryClient

logger = logging.getLogger(__name__)

def enrich_books_service(
    db_manager: DatabaseManager, 
    ol_client: OpenLibraryClient,
    limit: Optional[int] = None,
    batch_size: int = 50
) -> tuple[int, int]:
    """
    Enrich books in the main database using data from the Open Library client.
    
    Args:
        db_manager: Database manager instance
        ol_client: Open Library client instance
        limit: Max number of books to process
        batch_size: Batch size for processing
        
    Returns:
        tuple[int, int]: (enriched_count, failed_count)
    """
    # Get all books that don't have enrichment data yet
    logger.info("Fetching candidates for enrichment...")
    
    with db_manager.get_session() as session:
        stmt = select(Book.book_id, Book.title, Book.author)\
            .where(Book.ratings_average.is_(None))\
            .order_by(Book.book_id)
            
        if limit and limit > 0:
            stmt = stmt.limit(limit)
            
        # Fetch all candidates into memory to avoid holding DB lock during processing
        candidates = session.execute(stmt).all()
            
    total = len(candidates)
    logger.info(f"Found {total} books to enrich")
    
    enriched_count = 0
    failed_count = 0
    
    for i, (book_id, title, author) in enumerate(candidates, 1):
        if not title:
            continue
            
        if i % batch_size == 0:
            logger.info(f"Progress: [{i}/{total}]")
        
        try:
            enriched = ol_client.enrich_book(title, author or "Unknown")
            
            if enriched:
                # Update database (new session for short transaction)
                with db_manager.get_session() as session:
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
                logger.debug(f"Enriched '{title}': Rating={disp_rating}, Wanted={enriched.want_to_read_count}")
            else:
                failed_count += 1
                
        except Exception as e:
            failed_count += 1
            logger.error(f"Error enriching '{title}': {e}")
            
    logger.info(f"Enrichment complete. Enriched: {enriched_count}, Failed: {failed_count}")
    return enriched_count, failed_count
