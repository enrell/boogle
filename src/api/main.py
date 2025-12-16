from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
from src.sources import get_sources
from src.sources.types import SourceClient
from src.db import PostgresRepository

app = FastAPI(title="Boogle Metadata API", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sources = get_sources()
database: PostgresRepository | None = None


def get_database() -> PostgresRepository:
    global database
    if database is None:
        database = PostgresRepository()
    return database


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
    files: List[dict] = Field(default_factory=list)


class SearchResult(BaseModel):
    source: str
    book_id: str
    title: str
    url: str


@app.get("/")
async def root():
    return {"message": "Boogle Metadata Extraction API"}


def get_source_client(source: str) -> SourceClient:
    client = sources.get(source.lower())
    if not client:
        raise HTTPException(status_code=404, detail="Unsupported source")
    return client


@app.get("/metadata/{source}/{book_id}", response_model=BookMetadata)
async def get_metadata(source: str, book_id: str):
    try:
        db = get_database()
        client = get_source_client(source)
        cached_metadata = db.get_book(source, book_id)
        if cached_metadata:
            return cached_metadata
        metadata = client.extract_metadata(book_id)
        db.upsert_book(metadata)
        return metadata
    except HTTPException as exc:
        raise exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error extracting metadata: {str(e)}")


@app.get("/search", response_model=List[SearchResult])
async def search_books(query: str, limit: int = 10, source: Optional[str] = None):
    try:
        db = get_database()
        source_key = source.lower() if source else None
        if source_key:
            get_source_client(source_key)
        db_results = db.search_books(query, limit, source_key) if query else []
        results = list(db_results)

        if len(results) < limit:
            remaining = limit - len(results)
            remote_results = []
            target_clients = (
                [(source_key, get_source_client(source_key))]
                if source_key
                else list(sources.items())
            )
            for name, client in target_clients:
                remote_results.extend(client.search_books(query, remaining))
            existing_keys = {(result["source"], result["book_id"]) for result in results}
            for result in remote_results:
                key = (result["source"], result["book_id"])
                if key not in existing_keys:
                    results.append(result)
                    existing_keys.add(key)
            for remote in remote_results:
                if db.get_book(remote["source"], remote["book_id"]):
                    continue
                try:
                    client = get_source_client(remote["source"])
                    metadata = client.extract_metadata(remote["book_id"])
                    db.upsert_book(metadata)
                except Exception:
                    continue

        return results[:limit]
    except HTTPException as exc:
        raise exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching books: {str(e)}")


@app.get("/health")
async def health():
    return {"status": "healthy"}
