from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from src.scraper.scraper import GutenbergScraper

app = FastAPI(title="Gutenberg Metadata API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

scraper = GutenbergScraper()


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
    files: List[dict] = []


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
        metadata = scraper.extract_metadata(book_id)
        return metadata
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error extracting metadata: {str(e)}")


@app.get("/search", response_model=List[SearchResult])
async def search_books(query: str, limit: int = 10):
    try:
        results = scraper.search_books(query, limit)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching books: {str(e)}")


@app.get("/health")
async def health():
    return {"status": "healthy"}
