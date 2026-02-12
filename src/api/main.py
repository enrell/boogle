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

# Enhanced search imports
try:
    from src.search import EnhancedSearcher, CachedEnhancedSearcher

    ENHANCED_SEARCH_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Enhanced search not available: {e}")
    ENHANCED_SEARCH_AVAILABLE = False

# Global state
searcher = None
realtime_indexer = None
database = None
metadata_indexer = None
enhanced_searcher = None
use_realtime = False
use_light_mode = False
use_enhanced_search = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize global state on startup."""
    global \
        searcher, \
        realtime_indexer, \
        database, \
        metadata_indexer, \
        enhanced_searcher, \
        use_realtime, \
        use_light_mode, \
        use_enhanced_search

    index_dir = os.getenv("INDEX_DIR", "data/index")
    metadata_index_dir = os.getenv("METADATA_INDEX_DIR", "data/index_metadata")
    use_sqlite = os.getenv("USE_SQLITE", "0") == "1"
    use_realtime = os.getenv("REALTIME_INDEX", "0") == "1"
    use_light_mode = os.getenv("LIGHT_MODE", "0") == "1"
    use_enhanced_search = (
        os.getenv("ENHANCED_SEARCH", "1") == "1" and ENHANCED_SEARCH_AVAILABLE
    )

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

            # Initialize enhanced searcher if available
            if use_enhanced_search and ENHANCED_SEARCH_AVAILABLE:
                try:
                    print(
                        "Initializing enhanced search (spell correction + query expansion)..."
                    )
                    enhanced_searcher = CachedEnhancedSearcher(
                        index_dir=index_dir,
                        enable_spellcheck=True,
                        enable_expansion=True,
                        enable_language_detection=True,
                        expansion_boost=0.3,
                        cache_size=1000,
                        cache_ttl=300,
                    )
                    print("Enhanced search initialized successfully")
                except Exception as e:
                    print(f"Warning: Failed to initialize enhanced search: {e}")
                    use_enhanced_search = False
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


@app.get("/books")
async def list_books(
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    source: Optional[str] = Query(
        None, description="Filter by provider (e.g., gutenberg, openlibrary, pportal)"
    ),
):
    """
    Get all books with pagination.

    Returns a list of all books in the database, optionally filtered by source.
    Use pagination parameters to navigate through results.
    """
    if database is None:
        raise HTTPException(status_code=500, detail="Database not initialized")

    books = []
    if source:
        books = database.get_books_by_source(source, limit=limit, offset=offset)
    else:
        # Get books from all sources
        all_books = []
        for src in ["gutenberg", "openlibrary", "pportal", "internetarchive"]:
            src_books = database.get_books_by_source(src, limit=limit * 2)
            all_books.extend(src_books)
        books = all_books[offset : offset + limit]

    # Convert to API format
    results = []
    for book in books:
        results.append(_build_search_result(book, 1.0))

    return {
        "results": results,
        "meta": {
            "total": len(results),
            "limit": limit,
            "offset": offset,
            "source": source,
        },
    }


@app.get("/search")
async def search_books(
    query: str = Query(..., min_length=1, max_length=1000),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    # Provider filters
    sources: Optional[List[str]] = Query(
        None, description="Filter by providers (e.g., gutenberg, openlibrary, pportal)"
    ),
    exclude_sources: Optional[List[str]] = Query(
        None, description="Exclude specific providers"
    ),
    # Content filters
    languages: Optional[List[str]] = Query(
        None, description="Filter by language codes (ISO 639-1, e.g., en, pt, es)"
    ),
    formats: Optional[List[str]] = Query(
        None, description="Filter by available formats (txt, epub, pdf, html)"
    ),
    has_fulltext: Optional[bool] = Query(
        None, description="Only books with downloadable full text"
    ),
    # Quality filters
    min_completeness: float = Query(
        0.0, ge=0.0, le=1.0, description="Minimum metadata completeness (0.0-1.0)"
    ),
    min_rating: float = Query(
        0.0, ge=0.0, le=5.0, description="Minimum rating (0.0-5.0)"
    ),
    # Temporal filters
    year_from: Optional[int] = Query(
        None, ge=1000, le=2100, description="Minimum publication year"
    ),
    year_to: Optional[int] = Query(
        None, ge=1000, le=2100, description="Maximum publication year"
    ),
    # Subject filters
    subjects: Optional[List[str]] = Query(
        None, description="Filter by subjects/categories"
    ),
    subject_mode: str = Query(
        "any", description="Subject matching mode: any, all, or exact"
    ),
    # Result filters
    deduplicate: bool = Query(
        True, description="Remove duplicate books across providers"
    ),
):
    """
    Search books with filters and NLP enhancements.

    This endpoint automatically applies:
    - Language detection (75+ languages)
    - Spell correction (typos automatically fixed)
    - Query expansion (synonyms and related terms)

    Returns unified results with multiple sources per book.
    """
    if database is None:
        raise HTTPException(status_code=500, detail="Database not initialized")

    # Try enhanced search first (if available)
    if use_enhanced_search and enhanced_searcher is not None:
        try:
            # Validate query
            try:
                query = SecurityValidators.validate_query(query)
            except SecurityError as e:
                raise HTTPException(status_code=400, detail=str(e))

            # Use enhanced search with spell correction and expansion
            results, query_info = enhanced_searcher.search(
                query,
                top_k=limit,
                apply_spellcheck=True,
                apply_expansion=True,
            )

            # Convert to API format
            api_results = []
            for result in results:
                # Get metadata
                meta = None
                for source in ["gutenberg", "openlibrary", "pportal"]:
                    meta = database.get_book(source, result.book_id)
                    if meta:
                        break

                if meta:
                    api_results.append(_build_search_result(meta, result.score))

            # Build response with enhancement metadata
            response = {
                "results": api_results,
                "meta": {
                    "total": len(api_results),
                    "limit": limit,
                    "offset": offset,
                    "query": query_info.get("corrected_query", query),
                    "original_query": query_info["original_query"],
                },
                "enhancements": {
                    "language_detected": query_info.get("language"),
                    "spell_corrected": query_info.get("was_corrected", False),
                    "query_expanded": query_info.get("was_expanded", False),
                },
            }

            # Add spell corrections if any
            if query_info.get("was_corrected"):
                response["enhancements"]["spell_corrections"] = query_info.get(
                    "spell_corrections", {}
                )

            # Add expansions if any
            if query_info.get("was_expanded"):
                response["enhancements"]["expansions"] = query_info.get(
                    "expansions", {}
                )

            return response

        except Exception as e:
            # Log error and fall back to basic search
            print(f"Enhanced search error: {e}")
            pass  # Fall through to basic search

    # Fallback to basic search (original implementation)
    # Validate query
    try:
        query = SecurityValidators.validate_query(query)
    except SecurityError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Build search filters
    filters = SearchFilters(
        sources=sources,
        exclude_sources=exclude_sources,
        languages=languages,
        formats=formats,
        has_fulltext=has_fulltext,
        min_completeness=min_completeness,
        min_rating=min_rating,
        year_from=year_from,
        year_to=year_to,
        subjects=subjects,
        subject_mode=subject_mode,
        deduplicate=deduplicate,
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

    return {
        "results": results[:limit],
        "meta": {
            "total": len(results[:limit]),
            "limit": limit,
            "offset": offset,
            "query": query,
        },
        "enhancements": {
            "language_detected": None,
            "spell_corrected": False,
            "query_expanded": False,
        },
    }


def _database_search(
    query: str, limit: int, offset: int, filters: SearchFilters
) -> List[SearchResult]:
    """Fallback database search."""
    books = database.search_books(query, limit=limit * 10)  # Get more for filtering

    results = []
    skipped = 0

    for book in books:
        if len(results) >= limit:
            break

        # Skip if before offset
        if skipped < offset:
            skipped += 1
            continue

        # Apply filters
        source = book.get("source", "unknown")

        # Provider filters
        if filters.sources and source not in filters.sources:
            continue
        if filters.exclude_sources and source in filters.exclude_sources:
            continue

        # Content filters
        if filters.languages and book.get("language") not in filters.languages:
            continue

        # Temporal filters
        if filters.year_from and book.get("publication_year", 0) < filters.year_from:
            continue
        if filters.year_to and book.get("publication_year", 9999) > filters.year_to:
            continue

        # Subject filters
        if filters.subjects:
            book_subjects = book.get("subjects") or []
            if isinstance(book_subjects, str):
                book_subjects = [book_subjects]
            elif not isinstance(book_subjects, list):
                book_subjects = []
            if filters.subject_mode == "any":
                if not any(subj in book_subjects for subj in filters.subjects):
                    continue
            elif filters.subject_mode == "all":
                if not all(subj in book_subjects for subj in filters.subjects):
                    continue

        # Quality filters
        if filters.min_completeness > 0.0:
            completeness = book.get("metadata_completeness", 0.0)
            if isinstance(completeness, str):
                try:
                    completeness = float(completeness)
                except:
                    completeness = 0.0
            if completeness < filters.min_completeness:
                continue

        if filters.min_rating > 0.0:
            rating = book.get("ratings_average") or 0.0
            if isinstance(rating, str):
                try:
                    rating = float(rating)
                except:
                    rating = 0.0
            if rating < filters.min_rating:
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
        source = meta.get("source", "unknown")

        # Provider filters
        if filters.sources and source not in filters.sources:
            continue
        if filters.exclude_sources and source in filters.exclude_sources:
            continue

        # Content filters
        if filters.languages and meta.get("language") not in filters.languages:
            continue

        # Temporal filters
        if filters.year_from and meta.get("publication_year", 0) < filters.year_from:
            continue
        if filters.year_to and meta.get("publication_year", 9999) > filters.year_to:
            continue

        # Subject filters
        if filters.subjects:
            book_subjects = meta.get("subjects") or []
            if isinstance(book_subjects, str):
                book_subjects = [book_subjects]
            elif not isinstance(book_subjects, list):
                book_subjects = []

            if filters.subject_mode == "any":
                # At least one subject must match
                if not any(subj in book_subjects for subj in filters.subjects):
                    continue
            elif filters.subject_mode == "all":
                # All subjects must be present
                if not all(subj in book_subjects for subj in filters.subjects):
                    continue
            elif filters.subject_mode == "exact":
                # Exact match (same subjects in any order)
                if set(book_subjects) != set(filters.subjects):
                    continue

        # Quality filters
        if filters.min_completeness > 0.0:
            completeness = meta.get("metadata_completeness", 0.0)
            if isinstance(completeness, str):
                try:
                    completeness = float(completeness)
                except:
                    completeness = 0.0
            if completeness < filters.min_completeness:
                continue

        if filters.min_rating > 0.0:
            rating = meta.get("ratings_average") or 0.0
            if isinstance(rating, str):
                try:
                    rating = float(rating)
                except:
                    rating = 0.0
            if rating < filters.min_rating:
                continue

        # Format filters
        if filters.formats:
            files = meta.get("files", []) or []
            available_formats = [f.get("format", "").lower() for f in files]
            if not any(fmt.lower() in available_formats for fmt in filters.formats):
                continue

        # Full-text filter
        if filters.has_fulltext is not None:
            has_text = bool(meta.get("files"))
            if filters.has_fulltext and not has_text:
                continue
            if not filters.has_fulltext and has_text:
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
