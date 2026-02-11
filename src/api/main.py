"""
Boogle Search API - Unified Schema Version

Features:
- Unified search results with multiple sources
- Search filters (provider, language, year, etc.)
- Source selection for downloads
- Cross-reference merged metadata
"""

import os
import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
from dataclasses import asdict

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from src.db.database import PostgresRepository
from src.indexer.stopwords import load_stopwords
from src.schemas.translator_registry import TranslatorRegistry
from src.services.cross_reference import CrossReferenceService
from src.security.validators import SecurityValidators, SecurityError

# Import models from our models file
from src.api.models import (
    SearchResult,
    SearchFilters,
    BookDetailResponse,
    HealthResponse,
    ErrorResponse,
    SourceInfo,
    ContributorInfo,
    FileInfo,
)

# Global state
searcher = None
realtime_indexer = None
database = None
metadata_indexer = None
use_realtime = False
use_light_mode = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize global state on startup."""
    global \
        searcher, \
        realtime_indexer, \
        database, \
        metadata_indexer, \
        use_realtime, \
        use_light_mode

    index_dir = os.getenv("INDEX_DIR", "data/index")
    metadata_index_dir = os.getenv("METADATA_INDEX_DIR", "data/index_metadata")
    use_sqlite = os.getenv("USE_SQLITE", "0") == "1"
    use_realtime = os.getenv("REALTIME_INDEX", "0") == "1"
    use_light_mode = os.getenv("LIGHT_MODE", "0") == "1"

    try:
        from rust_bm25 import FileSearcher, RealTimeIndexer

        if use_light_mode:
            from src.indexer.metadata_indexer import MetadataIndexer

            metadata_indexer = MetadataIndexer(
                index_dir=metadata_index_dir, use_sqlite=use_sqlite
            )
            metadata_indexer.load_index()
            database = metadata_indexer.db
        elif use_realtime:
            realtime_indexer = RealTimeIndexer(index_dir)
            database = PostgresRepository(use_sqlite=use_sqlite)
        else:
            stopwords = list(load_stopwords())
            searcher = FileSearcher(index_dir)
            searcher.set_stopwords(stopwords)
            database = PostgresRepository(use_sqlite=use_sqlite)
    except ImportError:
        # Rust module not available
        database = PostgresRepository(use_sqlite=use_sqlite)

    yield

    # Cleanup
    if database:
        database.close()


app = FastAPI(
    title="Boogle Search API",
    version="2.1.0",
    description="Multi-provider book search with unified metadata",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """API root."""
    return {
        "message": "Boogle Search API",
        "version": "2.1.0",
        "features": ["multi-provider", "unified-schema", "source-selection"],
    }


@app.get("/providers")
async def list_providers():
    """List all available providers."""
    TranslatorRegistry.auto_discover()

    providers = []
    for name in TranslatorRegistry.list_providers():
        try:
            translator = TranslatorRegistry.get(name)
            providers.append(
                {
                    "name": name,
                    "quality_score": translator.source_quality,
                    "supports_downloads": hasattr(translator, "supports_downloads"),
                }
            )
        except:
            pass

    return {"providers": providers}


@app.get("/search", response_model=List[SearchResult])
async def search_books(
    query: str = Query(..., min_length=1, max_length=1000),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    # Filters
    sources: Optional[List[str]] = Query(None, description="Filter by providers"),
    languages: Optional[List[str]] = Query(
        None, description="Filter by language codes"
    ),
    year_from: Optional[int] = Query(None, ge=1000, le=2100),
    year_to: Optional[int] = Query(None, ge=1000, le=2100),
    subjects: Optional[List[str]] = Query(None, description="Filter by subjects"),
    min_completeness: float = Query(0.0, ge=0.0, le=1.0),
):
    """
    Search books with filters.

    Returns unified results with multiple sources per book.
    """
    if database is None:
        raise HTTPException(status_code=500, detail="Database not initialized")

    # Validate query
    try:
        query = SecurityValidators.validate_query(query)
    except SecurityError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Build search filters
    filters = SearchFilters(
        sources=sources,
        languages=languages,
        year_from=year_from,
        year_to=year_to,
        subjects=subjects,
        min_completeness=min_completeness,
        exclude_sources=None,
        formats=None,
        has_fulltext=None,
    )

    results = []

    if use_light_mode and metadata_indexer:
        # Light mode: metadata-only search
        raw_results = metadata_indexer.search(query, limit * 20)

        for book_id, score in raw_results[offset : offset + limit]:
            # Try to get from any source
            meta = None
            for source in ["gutenberg", "openlibrary", "pportal", "internetarchive"]:
                meta = database.get_book(source, book_id)
                if meta:
                    break

            if meta:
                results.append(_build_search_result(meta, score))

    else:
        # Full-text search
        try:
            if use_realtime and realtime_indexer:
                raw_results = realtime_indexer.search(query, (offset + limit) * 20)
            elif searcher:
                raw_results = searcher.search(query, (offset + limit) * 20)
            else:
                # Fallback to database search
                return _database_search(query, limit, offset, filters)

            # Process results with deduplication
            results = _process_search_results(raw_results, filters, limit, offset)

        except Exception as e:
            # Fallback to database search
            return _database_search(query, limit, offset, filters)

    return results[:limit]


def _database_search(
    query: str, limit: int, offset: int, filters: SearchFilters
) -> List[SearchResult]:
    """Fallback database search."""
    books = database.search_books(query, limit=limit * 2)

    results = []
    for book in books[offset : offset + limit]:
        # Apply filters
        if filters.sources and book.get("source") not in filters.sources:
            continue
        if filters.languages and book.get("language") not in filters.languages:
            continue
        if filters.year_from and book.get("publication_year", 0) < filters.year_from:
            continue
        if filters.year_to and book.get("publication_year", 9999) > filters.year_to:
            continue

        results.append(_build_search_result(book, 1.0))

    return results


def _process_search_results(
    raw_results: list, filters: SearchFilters, limit: int, offset: int
) -> List[SearchResult]:
    """Process raw search results with deduplication and filtering."""

    # Group by canonical_id (deduplication)
    unique_books: Dict[str, tuple[dict, float]] = {}

    for book_id, score, chunk_id in raw_results:
        # Get metadata from database
        meta = database.get_book("gutenberg", book_id)
        if not meta:
            # Try other sources
            for source in ["openlibrary", "pportal", "internetarchive"]:
                meta = database.get_book(source, book_id)
                if meta:
                    break

        if not meta:
            continue

        # Apply filters
        if filters.sources and meta.get("source") not in filters.sources:
            continue
        if filters.languages and meta.get("language") not in filters.languages:
            continue
        if filters.year_from and meta.get("publication_year", 0) < filters.year_from:
            continue
        if filters.year_to and meta.get("publication_year", 9999) > filters.year_to:
            continue

        # Use canonical_id for deduplication
        canonical_id = meta.get("canonical_id", f"{meta['source']}:{book_id}")

        # Keep highest score
        if canonical_id not in unique_books or score > unique_books[canonical_id][1]:
            unique_books[canonical_id] = (meta, score)

    # Sort by score
    sorted_books = sorted(unique_books.values(), key=lambda x: x[1], reverse=True)

    # Apply offset and limit
    sorted_books = sorted_books[offset : offset + limit]

    # Build results
    results = []
    for meta, score in sorted_books:
        results.append(_build_search_result(meta, score))

    return results


def _build_search_result(meta: dict, score: float) -> SearchResult:
    """Build a SearchResult from database metadata."""

    # Get all sources for this book
    all_sources = []
    primary_source = None

    # Try to get canonical_id
    canonical_id = meta.get(
        "canonical_id", f"{meta.get('source', 'unknown')}:{meta.get('book_id', '')}"
    )

    # Build sources list
    source = meta.get("source", "unknown")
    book_id = meta.get("book_id", "")

    primary_source = SourceInfo(
        provider=source,
        book_id=book_id,
        url=meta.get("url", f"https://example.com/{book_id}"),
        files=[FileInfo(format=meta.get("format", "txt"), url=meta.get("url", ""))]
        if meta.get("format")
        else None,
    )

    all_sources = [primary_source]

    return SearchResult(
        canonical_id=canonical_id,
        title=meta.get("title") or "Unknown",
        subtitle=meta.get("subtitle"),
        authors=[ContributorInfo(name=meta["author"], role="author")]
        if meta.get("author")
        else [],
        language=meta.get("language"),
        subjects=[cat] if (cat := meta.get("category")) else None,
        publication_year=meta.get("publication_year"),
        cover_url=meta.get("cover_url"),
        thumbnail_url=meta.get("cover_url"),  # Use cover_url as thumbnail
        metadata_completeness=meta.get("metadata_completeness", 0.5),
        primary_source=primary_source,
        all_sources=all_sources,
        source_count=1,
        score=score,
    )


@app.get("/book/{canonical_id}", response_model=BookDetailResponse)
async def get_book_details(canonical_id: str):
    """
    Get detailed information about a book by canonical ID.

    Includes all sources and download links.
    """
    if database is None:
        raise HTTPException(status_code=500, detail="Database not initialized")

    # Parse canonical_id
    if ":" in canonical_id:
        source, book_id = canonical_id.split(":", 1)
    else:
        # Fallback
        source, book_id = "gutenberg", canonical_id

    meta = database.get_book(source, book_id)

    if not meta:
        raise HTTPException(status_code=404, detail="Book not found")

    # Build detailed response
    return BookDetailResponse(
        canonical_id=canonical_id,
        title=meta.get("title") or "Unknown",
        subtitle=meta.get("subtitle"),
        authors=[ContributorInfo(name=meta["author"], role="author")]
        if meta.get("author")
        else [],
        language=meta.get("language"),
        subjects=[cat] if (cat := meta.get("category")) else None,
        publication_year=meta.get("publication_year"),
        description=meta.get("description"),
        cover_url=meta.get("cover_url"),
        thumbnail_url=meta.get("cover_url"),  # Use cover_url as thumbnail
        page_count=None,
        isbn_10=None,
        isbn_13=None,
        primary_source=SourceInfo(
            provider=source,
            book_id=book_id,
            url=meta.get("url", f"https://example.com/{book_id}"),
            files=None,
        ),
        all_sources=[
            SourceInfo(
                provider=source,
                book_id=book_id,
                url=meta.get("url", f"https://example.com/{book_id}"),
                files=[
                    FileInfo(format=meta.get("format", "txt"), url=meta.get("url", ""))
                ]
                if meta.get("format")
                else None,
            )
        ],
        source_count=1,
        metadata_completeness=meta.get("metadata_completeness", 0.5),
        indexed_at=meta.get("created_at") or datetime.now(),
    )


@app.get("/health")
async def health():
    """Health check endpoint."""
    if use_light_mode:
        mode = "light"
    elif use_realtime:
        mode = "realtime"
    else:
        mode = "batch"

    # Get provider info
    TranslatorRegistry.auto_discover()

    return {
        "status": "healthy",
        "mode": mode,
        "providers": TranslatorRegistry.list_providers(),
    }


# Legacy endpoints for backward compatibility
@app.get("/metadata/{source}/{book_id}")
async def get_metadata_legacy(source: str, book_id: str):
    """Legacy metadata endpoint (deprecated)."""
    if database is None:
        raise HTTPException(status_code=500, detail="Database not initialized")

    meta = database.get_book(source, book_id)
    if meta:
        return meta
    raise HTTPException(status_code=404, detail="Book not found")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
