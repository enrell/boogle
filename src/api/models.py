"""
API Pydantic Models

Strongly typed models for API requests and responses.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator
from datetime import datetime


class ContributorInfo(BaseModel):
    """Contributor (author, illustrator, etc.)"""

    name: str = Field(..., min_length=1, max_length=500)
    role: str = Field(
        default="author", pattern="^(author|illustrator|editor|translator)$"
    )


class FileInfo(BaseModel):
    """File download information"""

    format: str = Field(..., pattern="^(pdf|epub|txt|mobi|html)$")
    url: str = Field(..., max_length=2000)
    size: Optional[str] = None


class SourceInfo(BaseModel):
    """Provider source information"""

    provider: str = Field(..., pattern="^[a-z][a-z0-9_]*$")
    book_id: str = Field(..., max_length=100)
    url: str = Field(..., max_length=2000)
    files: Optional[List[FileInfo]] = None
    quality_score: float = Field(default=1.0, ge=0.0, le=1.0)


class RatingInfo(BaseModel):
    """Book rating information"""

    average: Optional[float] = Field(None, ge=0.0, le=5.0)
    count: Optional[int] = Field(None, ge=0)


class PopularityInfo(BaseModel):
    """Popularity metrics"""

    downloads: Optional[int] = Field(None, ge=0)
    views: Optional[int] = Field(None, ge=0)
    want_to_read: Optional[int] = Field(None, ge=0)


class SearchResult(BaseModel):
    """Unified search result"""

    canonical_id: str = Field(..., max_length=100)
    title: str = Field(..., min_length=1, max_length=5000)
    subtitle: Optional[str] = Field(None, max_length=5000)
    authors: Optional[List[ContributorInfo]] = None

    # Bibliographic
    language: Optional[str] = Field(None, pattern="^[a-z]{2}$")
    subjects: Optional[List[str]] = None
    publication_year: Optional[int] = Field(None, ge=1000, le=2100)

    # Media
    cover_url: Optional[str] = Field(None, max_length=2000)
    thumbnail_url: Optional[str] = Field(None, max_length=2000)

    # Quality
    metadata_completeness: float = Field(default=0.0, ge=0.0, le=1.0)
    rating: Optional[RatingInfo] = None
    popularity: Optional[PopularityInfo] = None

    # Sources
    primary_source: SourceInfo
    all_sources: List[SourceInfo]
    source_count: int = Field(default=1, ge=1)

    # Search
    score: float = Field(default=0.0, ge=0.0)

    class Config:
        json_schema_extra = {
            "example": {
                "canonical_id": "gutenberg:12345",
                "title": "Pride and Prejudice",
                "authors": [{"name": "Jane Austen", "role": "author"}],
                "language": "en",
                "subjects": ["Romance", "Classic"],
                "publication_year": 1813,
                "metadata_completeness": 0.85,
                "primary_source": {
                    "provider": "gutenberg",
                    "book_id": "12345",
                    "url": "https://www.gutenberg.org/ebooks/12345",
                },
                "all_sources": [
                    {"provider": "gutenberg", "book_id": "12345", "url": "..."},
                    {"provider": "openlibrary", "book_id": "OL12345W", "url": "..."},
                ],
                "source_count": 2,
                "score": 0.95,
            }
        }


class SearchFilters(BaseModel):
    """Search filters for API"""

    # Source filters
    sources: Optional[List[str]] = Field(None, description="Filter by providers")
    exclude_sources: Optional[List[str]] = Field(None, description="Exclude providers")

    # Content filters
    languages: Optional[List[str]] = Field(None, description="Filter by language codes")
    formats: Optional[List[str]] = Field(
        None, description="Filter by available formats"
    )
    has_fulltext: Optional[bool] = Field(
        None, description="Only books with downloadable text"
    )

    # Quality filters
    min_completeness: float = Field(default=0.0, ge=0.0, le=1.0)
    min_rating: float = Field(default=0.0, ge=0.0, le=5.0)

    # Temporal filters
    year_from: Optional[int] = Field(None, ge=1000, le=2100)
    year_to: Optional[int] = Field(None, ge=1000, le=2100)

    # Subject filters
    subjects: Optional[List[str]] = Field(None, description="Filter by subjects")
    subject_mode: str = Field(default="any", pattern="^(any|all|exact)$")

    # Result filters
    deduplicate: bool = Field(default=True)
    diversity_boost: bool = Field(default=True)

    @validator("year_to")
    def year_to_greater_than_from(cls, v, values):
        if v and values.get("year_from") and v < values["year_from"]:
            raise ValueError("year_to must be >= year_from")
        return v


class SearchRequest(BaseModel):
    """Search request with filters"""

    query: str = Field(..., min_length=1, max_length=1000)
    limit: int = Field(default=10, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
    filters: Optional[SearchFilters] = Field(default_factory=SearchFilters)

    @validator("query")
    def query_not_empty(cls, v):
        if not v.strip():
            raise ValueError("Query cannot be empty")
        return v.strip()


class BookDetailResponse(BaseModel):
    """Detailed book response"""

    canonical_id: str
    title: str
    subtitle: Optional[str] = None
    authors: Optional[List[ContributorInfo]] = None
    language: Optional[str] = None
    languages: Optional[List[str]] = None
    subjects: Optional[List[str]] = None
    categories: Optional[List[str]] = None

    publication_year: Optional[int] = None
    publisher: Optional[str] = None
    edition: Optional[str] = None

    description: Optional[str] = None
    abstract: Optional[str] = None
    page_count: Optional[int] = Field(None, ge=1)

    cover_url: Optional[str] = None
    thumbnail_url: Optional[str] = None

    copyright_status: Optional[str] = None
    license: Optional[str] = None

    isbn_10: Optional[str] = Field(None, pattern=r"^\d{9}[\dX]$")
    isbn_13: Optional[str] = Field(None, pattern=r"^\d{13}$")

    rating: Optional[RatingInfo] = None
    popularity: Optional[PopularityInfo] = None

    primary_source: SourceInfo
    all_sources: List[SourceInfo]
    source_count: int

    metadata_completeness: float
    indexed_at: datetime

    # Provider-specific metadata
    provider_metadata: Optional[Dict[str, Any]] = None


class HealthResponse(BaseModel):
    """Health check response"""

    status: str = Field(..., pattern="^(healthy|unhealthy|degraded)$")
    mode: str = Field(..., pattern="^(batch|realtime|light)$")
    version: str = "2.0.0"
    providers: List[str] = Field(default_factory=list)
    indexed_books: Optional[int] = None


class ErrorResponse(BaseModel):
    """Error response"""

    error: str
    detail: Optional[str] = None
    code: Optional[str] = None
