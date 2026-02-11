"""
OpenLibrary Translator

Translates Open Library metadata to UnifiedBookMetadata.
"""

from typing import Dict, Any, List

from src.schemas.translator import SchemaTranslator, TranslationContext
from src.schemas.unified_metadata import (
    Contributor,
    FileInfo,
    ProviderSource,
    ImageInfo,
    RatingInfo,
    PopularityInfo,
)


class OpenLibraryTranslator(SchemaTranslator):
    """Translator for Open Library metadata."""

    @property
    def provider_name(self) -> str:
        return "openlibrary"

    @property
    def source_quality(self) -> float:
        """OpenLibrary is a good quality curated source."""
        return 0.9

    def generate_canonical_id(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> str:
        """Use OpenLibrary work key as canonical ID."""
        work_key = raw.get("book_id", "")
        if work_key.startswith("/works/"):
            work_key = work_key[7:]
        return f"openlibrary:{work_key}"

    def translate_core(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate core fields."""
        work_key = raw.get("book_id", "")

        return {
            "canonical_id": self.generate_canonical_id(raw, ctx),
            "title": raw.get("title", "Unknown"),
            "language": raw.get("language"),  # May be None for works
            "source_url": raw.get("url", f"https://openlibrary.org/works/{work_key}"),
        }

    def translate_bibliographic(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate bibliographic fields."""
        # Authors - may be formatted as "Author 1, Author 2"
        authors = []
        if raw.get("author"):
            author_str = str(raw["author"])
            # Handle multiple authors
            if " and " in author_str:
                for part in author_str.split(" and "):
                    authors.extend(self.normalize_author(part.strip()))
            elif "," in author_str and len(author_str.split(",")) == 2:
                # Simple "Last, First" format
                authors.extend(self.normalize_author(author_str))
            else:
                authors.extend(self.normalize_author(author_str))

        # Subjects - OpenLibrary uses comma-separated subjects
        subjects = []
        if raw.get("category"):
            subjects = self.parse_subjects(raw["category"])

        # Publication year
        pub_year = None
        if raw.get("original_publication"):
            pub_year = self.extract_year(raw["original_publication"])

        return {
            "authors": authors,
            "subjects": subjects,
            "publication_date": raw.get("original_publication"),
            "publication_year": pub_year,
            "publisher": raw.get("publisher"),
        }

    def translate_content(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate content fields."""
        return {
            "description": raw.get("description"),
            "copyright_status": raw.get("copyright_status"),
        }

    def translate_media(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate media fields."""
        work_key = raw.get("book_id", "")

        # Cover image
        cover_image = None
        if raw.get("cover_url"):
            cover_image = ImageInfo(url=raw["cover_url"])
        elif work_key:
            # Try to construct cover URL from work key
            work_id = work_key.replace("/works/", "")
            cover_image = ImageInfo(
                url=f"https://covers.openlibrary.org/b/id/{work_id}-M.jpg"
            )

        # OpenLibrary doesn't provide direct file downloads
        files = []

        return {
            "files": files,
            "cover_image": cover_image,
            "thumbnail_url": cover_image.url if cover_image else None,
        }

    def translate_identifiers(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate identifier fields."""
        work_key = raw.get("book_id", "")
        work_id = work_key.replace("/works/", "") if work_key else ""

        return {
            "openlibrary_id": work_id,
            "provider_ids": {"openlibrary": work_id},
        }

    def translate_enrichment(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate enrichment fields."""
        rating = None
        popularity = None

        # Downloads (actually edition_count in OpenLibrary)
        if raw.get("downloads"):
            try:
                edition_count = int(raw["downloads"])
                popularity = PopularityInfo(downloads=edition_count)
            except (ValueError, TypeError):
                pass

        return {
            "rating": rating,
            "popularity": popularity,
        }
