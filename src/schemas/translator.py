"""
Schema Translator Base Class

Provides translation from provider-specific schemas to UnifiedBookMetadata.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from src.schemas.unified_metadata import (
    UnifiedBookMetadata,
    FileInfo,
    Contributor,
    ImageInfo,
    ProviderSource,
    SearchSignals,
)


@dataclass
class TranslationContext:
    """Context for translation decisions."""

    provider_name: str
    raw_metadata: Dict[str, Any]
    confidence_threshold: float = 0.7


class SchemaTranslator(ABC):
    """
    Abstract translator that converts provider-specific metadata to UnifiedBookMetadata.

    Providers must implement this to translate their native schema to the unified format.
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Return the provider name this translator handles."""
        pass

    @property
    @abstractmethod
    def source_quality(self) -> float:
        """
        Return quality score for this provider (0.0-1.0).

        Used for source ranking:
        - 1.0: Premium curated sources (Gutenberg)
        - 0.8: Good quality sources (OpenLibrary)
        - 0.6: Aggregated sources (PPORTAL)
        """
        pass

    @abstractmethod
    def translate_core(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """
        Translate core required fields.

        Must return:
        - canonical_id: str (for cross-provider deduplication)
        - title: str
        - primary_source: ProviderSource
        - language: str (ISO code)
        """
        pass

    @abstractmethod
    def translate_bibliographic(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """
        Translate bibliographic fields.

        Should return:
        - authors: List[Contributor]
        - subtitle: Optional[str]
        - subjects: List[str]
        - publication_year: Optional[int]
        - publisher: Optional[str]
        """
        pass

    @abstractmethod
    def translate_content(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """
        Translate content/description fields.

        Should return:
        - description: Optional[str]
        - abstract: Optional[str]
        - page_count: Optional[int]
        - copyright_status: Optional[str]
        """
        pass

    @abstractmethod
    def translate_media(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """
        Translate media fields.

        Should return:
        - cover_image: Optional[ImageInfo]
        - thumbnail_url: Optional[str]
        - files: List[FileInfo]
        """
        pass

    @abstractmethod
    def translate_identifiers(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """
        Translate identifier fields.

        Should return:
        - isbn_10: Optional[str]
        - isbn_13: Optional[str]
        - provider_ids: Dict[str, str]
        """
        pass

    @abstractmethod
    def translate_enrichment(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """
        Translate enrichment/popularity fields.

        Should return:
        - rating: Optional[RatingInfo]
        - popularity: Optional[PopularityInfo]
        """
        pass

    def generate_canonical_id(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> str:
        """
        Generate a canonical ID for cross-provider deduplication.

        Default implementation uses title + author hash.
        Override for provider-specific canonical IDs.
        """
        import hashlib

        title = str(raw.get("title", "")).lower().strip()
        author = str(raw.get("author", raw.get("authors", ""))).lower().strip()

        if title and author:
            key = f"{title}|{author}"
        else:
            key = f"{ctx.provider_name}:{raw.get('book_id', '')}"

        return hashlib.md5(key.encode()).hexdigest()[:16]

    def calculate_completeness(self, unified: Dict[str, Any]) -> float:
        """Calculate metadata completeness score."""
        score = 0.0

        # Core fields (50%)
        core_fields = ["title", "authors", "language"]
        core_score = sum(1 for f in core_fields if unified.get(f)) / len(core_fields)
        score += core_score * 0.5

        # Standard fields (30%)
        standard_fields = ["subjects", "publication_year", "description", "cover_image"]
        std_score = sum(1 for f in standard_fields if unified.get(f)) / len(
            standard_fields
        )
        score += std_score * 0.3

        # Extended fields (20%)
        extended_fields = ["page_count", "isbn_13", "rating", "popularity"]
        ext_score = sum(1 for f in extended_fields if unified.get(f)) / len(
            extended_fields
        )
        score += ext_score * 0.2

        return min(1.0, score)

    def translate(self, raw: Dict[str, Any]) -> UnifiedBookMetadata:
        """
        Main translation method - orchestrates all phases.

        This is the entry point that providers call.
        """
        ctx = TranslationContext(provider_name=self.provider_name, raw_metadata=raw)

        # Phase 1: Core fields
        core = self.translate_core(raw, ctx)

        # Phase 2: Bibliographic fields
        biblio = self.translate_bibliographic(raw, ctx)

        # Phase 3: Content fields
        content = self.translate_content(raw, ctx)

        # Phase 4: Media fields
        media = self.translate_media(raw, ctx)

        # Phase 5: Identifiers
        identifiers = self.translate_identifiers(raw, ctx)

        # Phase 6: Enrichment
        enrichment = self.translate_enrichment(raw, ctx)

        # Merge all fields
        unified = {**core, **biblio, **content, **media, **identifiers, **enrichment}

        # Generate canonical_id if not provided
        if not unified.get("canonical_id"):
            unified["canonical_id"] = self.generate_canonical_id(raw, ctx)

        # Calculate completeness
        unified["metadata_completeness"] = self.calculate_completeness(unified)

        # Set search signals
        unified["search_signals"] = SearchSignals(
            metadata_completeness=unified["metadata_completeness"],
            source_quality_score=self.source_quality,
        )

        # Build ProviderSource for primary
        primary_source = unified.get("primary_source")
        if not primary_source:
            primary_source = ProviderSource(
                provider=self.provider_name,
                book_id=str(raw.get("book_id", "")),
                url=unified.get("source_url", ""),
                quality_score=self.source_quality,
            )
        unified["primary_source"] = primary_source

        # Build all_sources list
        all_sources = unified.get("all_sources", [])
        if not all_sources:
            all_sources = [unified["primary_source"]]
        unified["all_sources"] = all_sources
        unified["source_count"] = len(all_sources)

        # Remove temporary fields not in UnifiedBookMetadata
        unified.pop("source_url", None)
        unified.pop("files", None)  # files are now in ProviderSource

        return UnifiedBookMetadata(**unified)

    def normalize_author(self, author_str: Optional[str]) -> List[Contributor]:
        """Normalize author string to list of Contributors."""
        if not author_str:
            return []

        # Handle "Last, First" format
        if "," in author_str:
            parts = author_str.split(",")
            name = f"{parts[1].strip()} {parts[0].strip()}"
        else:
            name = author_str.strip()

        # Remove extra whitespace
        name = " ".join(name.split())

        if name and name.lower() != "unknown":
            return [Contributor(name=name, role="author")]
        return []

    def parse_subjects(
        self, subjects_str: Optional[str], delimiter: str = ","
    ) -> List[str]:
        """Parse subjects string to list."""
        if not subjects_str:
            return []

        subjects = []
        for s in subjects_str.split(delimiter):
            s = s.strip()
            if s and len(s) > 2:
                subjects.append(s)
        return subjects

    def extract_year(self, date_str: Optional[str]) -> Optional[int]:
        """Extract year from date string."""
        if not date_str:
            return None

        import re

        match = re.search(r"\b(19|20)\d{2}\b", str(date_str))
        if match:
            return int(match.group(0))
        return None


class FallbackTranslator(SchemaTranslator):
    """
    Fallback translator for providers without custom implementation.

    Uses generic heuristics to extract common fields.
    """

    @property
    def provider_name(self) -> str:
        return "fallback"

    @property
    def source_quality(self) -> float:
        return 0.5

    def translate_core(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {
            "title": str(raw.get("title", "") or "Unknown"),
            "language": raw.get("language", "en"),
            "source_url": raw.get("url", ""),  # Will be moved to ProviderSource
        }

    def translate_bibliographic(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {
            "authors": self.normalize_author(raw.get("author")),
            "subjects": self.parse_subjects(raw.get("category")),
            "publication_year": self.extract_year(raw.get("release_date")),
        }

    def translate_content(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {
            "description": raw.get("description"),
            "copyright_status": raw.get("copyright_status"),
        }

    def translate_media(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        files = []
        for f in raw.get("files", []):
            if isinstance(f, dict):
                files.append(
                    FileInfo(format=f.get("format", "unknown"), url=f.get("url", ""))
                )
        return {"files": files}

    def translate_identifiers(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {"provider_ids": {ctx.provider_name: str(raw.get("book_id", ""))}}

    def translate_enrichment(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {}
