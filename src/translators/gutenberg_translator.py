"""
Gutenberg Translator

Translates Project Gutenberg metadata to UnifiedBookMetadata.
"""

from typing import Dict, Any, List

from src.schemas.translator import SchemaTranslator, TranslationContext
from src.schemas.unified_metadata import (
    Contributor,
    FileInfo,
    ProviderSource,
    SearchSignals,
    ImageInfo,
    RatingInfo,
    PopularityInfo,
)


class GutenbergTranslator(SchemaTranslator):
    """Translator for Project Gutenberg metadata."""

    @property
    def provider_name(self) -> str:
        return "gutenberg"

    @property
    def source_quality(self) -> float:
        """Gutenberg is a premium curated source."""
        return 1.0

    def generate_canonical_id(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> str:
        """Use Gutenberg book ID as canonical ID."""
        return f"gutenberg:{raw.get('book_id', '')}"

    def translate_core(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate core fields."""
        book_id = str(raw.get("book_id", ""))

        return {
            "canonical_id": self.generate_canonical_id(raw, ctx),
            "title": raw.get("title", "Unknown"),
            "language": self._normalize_language(raw.get("language", "en")),
            "source_url": raw.get("url", f"https://www.gutenberg.org/ebooks/{book_id}"),
        }

    def translate_bibliographic(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate bibliographic fields."""
        # Authors
        authors = []
        if raw.get("author"):
            authors.extend(self.normalize_author(raw["author"]))
        if raw.get("illustrator"):
            authors.append(Contributor(name=raw["illustrator"], role="illustrator"))

        # Subjects
        subjects = []
        if raw.get("category"):
            subjects = self.parse_subjects(raw["category"])

        # Publication date (Gutenberg release date, not original)
        pub_year = None
        if raw.get("release_date"):
            pub_year = self.extract_year(raw["release_date"])

        return {
            "authors": authors,
            "subjects": subjects,
            "publication_date": raw.get("release_date"),
            "publication_year": pub_year,
            "publisher": "Project Gutenberg",
        }

    def translate_content(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate content fields."""
        return {
            "description": None,  # Gutenberg doesn't provide descriptions
            "copyright_status": raw.get("copyright_status", "Public Domain"),
        }

    def translate_media(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate media fields."""
        book_id = str(raw.get("book_id", ""))

        # Files
        files = []
        for f in raw.get("files", []):
            if isinstance(f, dict):
                files.append(
                    FileInfo(
                        format=f.get("format", "unknown").lower(), url=f.get("url", "")
                    )
                )

        # Cover image
        cover_image = None
        if book_id:
            cover_image = ImageInfo(
                url=f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.cover.medium.jpg"
            )

        return {
            "files": files,
            "cover_image": cover_image,
            "thumbnail_url": cover_image.url if cover_image else None,
        }

    def translate_identifiers(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate identifier fields."""
        book_id = str(raw.get("book_id", ""))

        return {
            "provider_ids": {"gutenberg": book_id},
        }

    def translate_enrichment(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate enrichment fields."""
        # Parse downloads as popularity signal
        popularity = None
        if raw.get("downloads"):
            try:
                downloads_str = str(raw["downloads"]).replace(",", "").replace(".", "")
                downloads = int(downloads_str)
                popularity = PopularityInfo(downloads=downloads)
            except (ValueError, TypeError):
                pass

        return {
            "popularity": popularity,
        }

    def _normalize_language(self, lang: str) -> str:
        """Normalize language code."""
        if not lang:
            return "en"

        lang = lang.lower().strip()

        # Common Gutenberg language names to ISO codes
        lang_map = {
            "english": "en",
            "french": "fr",
            "german": "de",
            "spanish": "es",
            "italian": "it",
            "portuguese": "pt",
            "dutch": "nl",
            "chinese": "zh",
            "russian": "ru",
            "latin": "la",
            "greek": "el",
            "arabic": "ar",
            "hebrew": "he",
        }

        return lang_map.get(lang, lang[:2] if len(lang) >= 2 else "en")
