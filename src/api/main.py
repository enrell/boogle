from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
from contextlib import asynccontextmanager

from src.db.database import PostgresRepository
from src.indexer.storage import IndexStorage
from src.indexer.ranker import Ranker


storage: IndexStorage | None = None
ranker: Ranker | None = None
database: PostgresRepository | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global storage, ranker, database
    storage = IndexStorage()
    ranker = Ranker(storage)
    database = PostgresRepository()
    yield
    if storage:
        storage.close()


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
    try:
        if database is None:
            raise HTTPException(status_code=500, detail="Database not initialized")
        cached_metadata = database.get_book(source, book_id)
        if cached_metadata:
            return cached_metadata
        raise HTTPException(status_code=404, detail="Book not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@app.get("/search", response_model=List[SearchResult])
async def search_books(query: str, limit: int = 10):
    try:
        if ranker is None:
            raise HTTPException(status_code=500, detail="Search not initialized")
        
        results = ranker.search(query, limit)
        
        return [
            SearchResult(
                book_id=str(r.book_id),
                title=r.title or "Unknown",
                author=r.author or "Unknown",
                score=r.score,
                url=f"https://www.gutenberg.org/ebooks/{r.book_id}"
            )
            for r in results
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching: {str(e)}")


@app.get("/health")
async def health():
    return {"status": "healthy"}
