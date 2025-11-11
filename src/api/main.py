from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
from src.scraper.scraper import GutenbergScraper
from src.db import SQLiteRepository

app = FastAPI(title="Gutenberg Metadata API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

scraper = GutenbergScraper()
database = SQLiteRepository()


class BookMetadata(BaseModel):
    book_id: int
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
    files: List[dict] = Field(default_factory=list)


class SearchResult(BaseModel):
    book_id: int
    title: str
    url: str


@app.get("/")
async def root():
    return {"message": "Gutenberg Metadata Extraction API"}


@app.get("/metadata/{book_id}", response_model=BookMetadata)
async def get_metadata(book_id: int):
    try:
        cached_metadata = database.get_book(book_id)
        if cached_metadata:
            return cached_metadata
        metadata = scraper.extract_metadata(book_id)
        database.upsert_book(metadata)
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error extracting metadata: {str(e)}")


@app.get("/search", response_model=List[SearchResult])
async def search_books(query: str, limit: int = 10):
    try:
        db_results = database.search_books(query, limit) if query else []
        results = list(db_results)

        if len(results) < limit:
            remaining = limit - len(results)
            remote_results = scraper.search_books(query, remaining)
            existing_ids = {result["book_id"] for result in results}
            for result in remote_results:
                if result["book_id"] not in existing_ids:
                    results.append(result)
                    existing_ids.add(result["book_id"])
            # opportunistically cache metadata for the newly discovered books
            for remote in remote_results:
                if database.get_book(remote["book_id"]):
                    continue
                try:
                    metadata = scraper.extract_metadata(remote["book_id"])
                    database.upsert_book(metadata)
                except Exception:
                    continue

        return results[:limit]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching books: {str(e)}")


@app.get("/health")
async def health():
    return {"status": "healthy"}
