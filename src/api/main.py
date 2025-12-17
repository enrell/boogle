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
    
    book_scores: dict[str, tuple[float, int]] = {}
    for book_id, score, chunk_id in raw_results:
        if book_id not in book_scores or score > book_scores[book_id][0]:
            book_scores[book_id] = (score, chunk_id)
    
    sorted_books = sorted(book_scores.items(), key=lambda x: x[1][0], reverse=True)[:limit]
    
    results = []
    for book_id, (score, chunk_id) in sorted_books:
        meta = database.get_book("gutenberg", book_id)
        if meta:
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

