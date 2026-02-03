import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
from contextlib import asynccontextmanager
from collections import defaultdict

from rust_bm25 import FileSearcher
from src.db.database import PostgresRepository
from src.indexer.stopwords import load_stopwords


searcher: FileSearcher | None = None
database: PostgresRepository | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global searcher, database
    index_dir = os.getenv("INDEX_DIR", "data/index")
    use_sqlite = os.getenv("USE_SQLITE", "0") == "1"
    stopwords = list(load_stopwords())
    
    searcher = FileSearcher(index_dir)
    searcher.set_stopwords(stopwords)
    database = PostgresRepository(use_sqlite=use_sqlite)
    yield


app = FastAPI(title="Boogle Search API", version="2.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class BookMetadata(BaseModel):
    source: str
    book_id: str
    url: str
    title: Optional[str] = None
    author: Optional[str] = None
    illustrator: Optional[str] = None
    release_date: Optional[str] = None
    language: Optional[str] = None
    category: Optional[str] = None
    original_publication: Optional[str] = None
    credits: Optional[str] = None
    copyright_status: Optional[str] = None
    downloads: Optional[str] = None
    cover_url: Optional[str] = None
    files: List[dict] = Field(default_factory=list)


class SearchResult(BaseModel):
    book_id: str
    title: str
    author: str
    score: float
    url: str


@app.get("/")
async def root():
    return {"message": "Boogle Search API"}


@app.get("/metadata/{source}/{book_id}", response_model=BookMetadata)
async def get_metadata(source: str, book_id: str):
    if database is None:
        raise HTTPException(status_code=500, detail="Database not initialized")
    cached_metadata = database.get_book(source, book_id)
    if cached_metadata:
        return cached_metadata
    raise HTTPException(status_code=404, detail="Book not found")


@app.get("/search", response_model=List[SearchResult])
async def search_books(query: str, limit: int = 10):
    if searcher is None or database is None:
        raise HTTPException(status_code=500, detail="Search not initialized")
    
    raw_results = searcher.search(query, limit * 20)
    
    candidate_ids = {r[0] for r in raw_results}
    candidates_meta = {}
    
    for bid in candidate_ids:
        meta = database.get_book("gutenberg", bid)
        if meta:
            candidates_meta[bid] = meta
            
    unique_books: dict[tuple[str, str], tuple[float, str]] = {}
    
    query_norm = query.lower()
    query_tokens = set(query_norm.split())
    
    for book_id, base_score, chunk_id in raw_results:
        meta = candidates_meta.get(book_id)
        if not meta:
            continue
            
        title = meta.get("title") or "Unknown"
        author = meta.get("author") or "Unknown"
        
        # Aggressive normalization for deduplication
        # Remove all punctuation and extra whitespace
        title_norm = " ".join("".join(c for c in title.lower() if c.isalnum() or c.isspace()).split())
        author_norm = " ".join("".join(c for c in author.lower() if c.isalnum() or c.isspace()).split())
        dedupe_key = (title_norm, author_norm)
        
        final_score = base_score
        
        if query_norm in title.lower():
            final_score *= 1.5
            
        # If any query token appears in author name, assume author search
        author_tokens = set(author_norm.split())
        if query_tokens & author_tokens:  # Intersection
            final_score *= 2.0
            
        # Popularity from Open Library (1.0x - 2.0x)
        ratings_avg = meta.get("ratings_average")
        ratings_count = meta.get("ratings_count")
        want_to_read = meta.get("want_to_read_count")
        edition_count = meta.get("edition_count")
        
        if any([ratings_avg, want_to_read, edition_count]):
            from src.enrichment.openlibrary import EnrichedMetadata
            enriched = EnrichedMetadata(
                ratings_average=ratings_avg,
                ratings_count=ratings_count,
                want_to_read_count=want_to_read,
                edition_count=edition_count
            )
            final_score *= enriched.popularity_score()
             
        # Keep best version of this book
        if dedupe_key not in unique_books or final_score > unique_books[dedupe_key][0]:
            unique_books[dedupe_key] = (final_score, book_id)
            
    # Sort & Format
    sorted_unique = sorted(unique_books.values(), key=lambda x: x[0], reverse=True)[:limit]
    
    results = []
    for score, book_id in sorted_unique:
        meta = candidates_meta[book_id]
        results.append(SearchResult(
            book_id=book_id,
            title=meta.get("title") or "Unknown",
            author=meta.get("author") or "Unknown",
            score=score,
            url=f"https://www.gutenberg.org/ebooks/{book_id}"
        ))
    
    return results


@app.get("/health")
async def health():
    return {"status": "healthy"}

