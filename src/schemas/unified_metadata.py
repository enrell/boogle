"""
Unified Book Metadata Schema

Standardized data model for cross-collection search and ranking.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime


@dataclass
class FileInfo:
    """Represents a downloadable file format."""

    format: str
    url: str
    size: Optional[str] = None
    checksum: Optional[str] = None


@dataclass
class Contributor:
    """A person who contributed to the work."""

    name: str
    role: str = "author"  # author, editor, translator, illustrator, etc.


@dataclass
class ImageInfo:
    """Image metadata."""

    url: str
    width: Optional[int] = None
    height: Optional[int] = None
    format: Optional[str] = None


@dataclass
class RatingInfo:
    """User ratings from various sources."""

    average: Optional[float] = None
    count: Optional[int] = None
    scale: float = 5.0


@dataclass
class PopularityInfo:
    """Popularity signals across sources."""

    downloads: Optional[int] = None
    views: Optional[int] = None
    want_to_read: Optional[int] = None
    currently_reading: Optional[int] = None
    shelves: Optional[int] = None


@dataclass
class ProviderSource:
    """Represents a single source provider for this book."""

    provider: str
    book_id: str
    url: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    files: List[FileInfo] = field(default_factory=list)
    quality_score: float = 1.0


@dataclass
class SearchSignals:
    """Computed signals for search ranking."""

    metadata_completeness: float = 0.0  # 0.0-1.0
    source_quality_score: float = 0.8  # Provider reliability
    relevance_boost: float = 1.0  # Computed from matching
    popularity_score: float = 1.0  # Computed from popularity signals
    quality_penalty: float = 1.0  # For low-quality matches


@dataclass
class UnifiedBookMetadata:
    """
    Unified metadata schema for cross-collection book search.

    This schema aggregates metadata from multiple sources (Gutenberg, OpenLibrary, PPORTAL, etc.)
    and provides a normalized view while preserving provider-specific data.
    """

    # Core Identification (Required)
    canonical_id: str  # Cross-provider deduplication ID
    title: str

    # Primary Source (Best quality source used for ranking)
    primary_source: ProviderSource

    # All Sources (for cross-reference and user selection)
    all_sources: List[ProviderSource] = field(default_factory=list)
    source_count: int = 1  # Number of providers having this book

    # Core Content
    subtitle: Optional[str] = None
    authors: List[Contributor] = field(default_factory=list)

    # Bibliographic
    language: Optional[str] = None
    languages: List[str] = field(default_factory=list)
    subjects: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    genres: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    # Publication
    publication_date: Optional[str] = None
    publication_year: Optional[int] = None
    publisher: Optional[str] = None
    edition: Optional[str] = None

    # Description
    description: Optional[str] = None
    abstract: Optional[str] = None
    table_of_contents: Optional[str] = None

    # Physical/Digital
    page_count: Optional[int] = None
    word_count: Optional[int] = None

    # Media
    cover_image: Optional[ImageInfo] = None
    thumbnail_url: Optional[str] = None
    preview_url: Optional[str] = None

    # Rights
    license: Optional[str] = None
    rights: Optional[str] = None
    copyright_status: Optional[str] = None

    # Identifiers
    isbn_10: Optional[str] = None
    isbn_13: Optional[str] = None
    oclc: Optional[str] = None

    # Series
    series: Optional[str] = None
    series_number: Optional[float] = None

    # Quality & Enrichment
    metadata_completeness: float = 0.0
    rating: Optional[RatingInfo] = None
    popularity: Optional[PopularityInfo] = None

    # Search Signals
    search_signals: SearchSignals = field(default_factory=SearchSignals)

    # Cross-Provider References
    provider_ids: Dict[str, str] = field(default_factory=dict)  # source -> book_id

    # Timestamps
    indexed_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def get_best_source(
        self, preferred_sources: Optional[List[str]] = None
    ) -> ProviderSource:
        """Get the best source by quality score, optionally filtered by preference list."""
        sources = self.all_sources if self.all_sources else [self.primary_source]

        # Sort by quality score descending
        sorted_sources = sorted(sources, key=lambda s: s.quality_score, reverse=True)

        if preferred_sources:
            for preferred in preferred_sources:
                for source in sorted_sources:
                    if source.provider == preferred:
                        return source

        # Return highest quality source
        return sorted_sources[0] if sorted_sources else self.primary_source

    def get_source(self, provider: str) -> Optional[ProviderSource]:
        """Get a specific source by provider name."""
        for source in self.all_sources:
            if source.provider == provider:
                return source
        return None

    def add_source(self, source: ProviderSource) -> None:
        """Add an additional source and update primary if better."""
        # Initialize all_sources if empty with primary
        if not self.all_sources:
            self.all_sources = [self.primary_source]

        # Avoid duplicates
        existing_providers = {s.provider for s in self.all_sources}
        if source.provider not in existing_providers:
            self.all_sources.append(source)

        self.source_count = len(self.all_sources)

        # Update primary if this source has better quality
        if source.quality_score > self.primary_source.quality_score:
            self.primary_source = source

        # Update provider_ids
        self.provider_ids[source.provider] = source.book_id

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "canonical_id": self.canonical_id,
            "title": self.title,
            "subtitle": self.subtitle,
            "authors": [{"name": c.name, "role": c.role} for c in self.authors]
            if self.authors
            else None,
            "language": self.language,
            "languages": self.languages if self.languages else None,
            "subjects": self.subjects if self.subjects else None,
            "publication_year": self.publication_year,
            "description": self.description,
            "page_count": self.page_count,
            "copyright_status": self.copyright_status,
            "cover_image": {
                "url": self.cover_image.url,
                "width": self.cover_image.width,
                "height": self.cover_image.height,
            }
            if self.cover_image
            else None,
            "thumbnail_url": self.thumbnail_url,
            "rating": {"average": self.rating.average, "count": self.rating.count}
            if self.rating
            else None,
            "popularity": {
                "downloads": self.popularity.downloads if self.popularity else None,
                "want_to_read": self.popularity.want_to_read
                if self.popularity
                else None,
            },
            # Sources for API selection
            "sources": [
                {
                    "provider": s.provider,
                    "book_id": s.book_id,
                    "url": s.url,
                    "files": [{"format": f.format, "url": f.url} for f in s.files]
                    if s.files
                    else None,
                }
                for s in self.all_sources
            ],
            "primary_source": {
                "provider": self.primary_source.provider,
                "book_id": self.primary_source.book_id,
                "url": self.primary_source.url,
            },
            "source_count": self.source_count,
            "metadata_completeness": round(self.metadata_completeness, 2),
        }

    def calculate_completeness(self) -> float:
        """Calculate metadata completeness score."""
        core_fields = [
            self.title is not None,
            len(self.authors) > 0,
            self.language is not None,
        ]

        standard_fields = [
            len(self.subjects) > 0,
            self.publication_year is not None,
            self.description is not None,
            self.cover_image is not None,
        ]

        extended_fields = [
            self.page_count is not None,
            self.isbn_13 is not None,
            self.rating is not None,
        ]

        core_score = sum(core_fields) / len(core_fields)
        std_score = sum(standard_fields) / len(standard_fields)
        ext_score = sum(extended_fields) / len(extended_fields)

        return core_score * 0.5 + std_score * 0.3 + ext_score * 0.2


# Type aliases for convenience
BookMetadata = UnifiedBookMetadata
