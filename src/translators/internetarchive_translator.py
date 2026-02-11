"""
Internet Archive Book Provider Translator

Fetches books from Internet Archive's Open Library.

Internet Archive provides:
- Millions of scanned books
- Multiple formats (PDF, EPUB, TXT)
- Rich metadata
- Public domain and creative commons works

API: https://archive.org/advancedsearch.php
Download: https://archive.org/download/{identifier}/
"""

from typing import Dict, Any, List, Optional

from src.schemas.translator import SchemaTranslator, TranslationContext
from src.schemas.unified_metadata import (
    Contributor,
    FileInfo,
    ImageInfo,
    PopularityInfo,
    RatingInfo,
)


class InternetArchiveTranslator(SchemaTranslator):
    """
    Translator for Internet Archive metadata.

    Internet Archive uses a simple metadata schema with these common fields:
    - identifier: unique ID (e.g., "walden00thor")
    - title: book title
    - creator: author(s)
    - date: publication date
    - subject: subjects (semicolon-separated)
    - description: description
    - licenseurl: license URL
    - downloads: download count
    """

    @property
    def provider_name(self) -> str:
        return "internetarchive"

    @property
    def source_quality(self) -> float:
        """
        Internet Archive is well-curated but metadata varies.
        Quality: 0.85 (good but not perfect)
        """
        return 0.85

    def translate_core(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate core fields."""
        identifier = str(raw.get("identifier", ""))

        return {
            "canonical_id": self.generate_canonical_id(raw, ctx),
            "title": raw.get("title") or "Unknown",
            "language": self._normalize_language(raw.get("language")),
            "source_url": f"https://archive.org/details/{identifier}",
        }

    def translate_bibliographic(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate bibliographic fields."""
        # Authors - Internet Archive uses 'creator' field
        authors = []
        creator = raw.get("creator")
        if creator:
            # Can be semicolon-separated for multiple authors
            if ";" in str(creator):
                for name in str(creator).split(";"):
                    authors.extend(self.normalize_author(name.strip()))
            else:
                authors.extend(self.normalize_author(str(creator)))

        # Subjects - semicolon-separated
        subjects = []
        if raw.get("subject"):
            subjects = self.parse_subjects(raw["subject"], delimiter=";")

        return {
            "authors": authors,
            "subjects": subjects,
            "publication_date": raw.get("date"),
            "publication_year": self.extract_year(raw.get("date")),
            "publisher": raw.get("publisher"),
        }

    def translate_content(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate content fields."""
        description = raw.get("description")

        # Extract license from licenseurl
        copyright_status = None
        license_url = raw.get("licenseurl", "")
        if "publicdomain" in license_url.lower():
            copyright_status = "Public Domain"
        elif "creativecommons" in license_url.lower():
            copyright_status = "Creative Commons"

        return {
            "description": description,
            "copyright_status": copyright_status,
        }

    def translate_media(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate media (files, covers)."""
        identifier = str(raw.get("identifier", ""))

        # Build available files
        files = []

        # Internet Archive provides these common formats
        formats = [
            ("pdf", "PDF"),
            ("epub", "EPUB"),
            ("txt", "Text"),
            ("mobi", "Kindle"),
        ]

        for fmt, label in formats:
            url = (
                f"https://archive.org/download/{identifier}/{identifier}_{label}.{fmt}"
            )
            files.append(FileInfo(format=fmt, url=url))

        # Cover image
        cover_image = None
        thumbnail_url = f"https://archive.org/services/img/{identifier}"

        return {
            "files": files,
            "cover_image": ImageInfo(url=thumbnail_url) if identifier else None,
            "thumbnail_url": thumbnail_url if identifier else None,
        }

    def translate_identifiers(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate identifier fields."""
        identifier = str(raw.get("identifier", ""))

        return {
            "provider_ids": {"internetarchive": identifier},
        }

    def translate_enrichment(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate enrichment fields."""
        popularity = None

        # Downloads count
        downloads = raw.get("downloads")
        if downloads:
            try:
                downloads_int = int(downloads)
                popularity = PopularityInfo(downloads=downloads_int)
            except (ValueError, TypeError):
                pass

        return {
            "popularity": popularity,
        }

    def _normalize_language(self, lang: Optional[str]) -> str:
        """Normalize language to ISO code."""
        if not lang:
            return "en"

        lang = str(lang).lower().strip()

        # Common Internet Archive language values
        lang_map = {
            "eng": "en",
            "english": "en",
            "spa": "es",
            "spanish": "es",
            "fre": "fr",
            "french": "fr",
            "ger": "de",
            "german": "de",
            "ita": "it",
            "italian": "it",
            "por": "pt",
            "portuguese": "pt",
            "lat": "la",
            "latin": "la",
            "ara": "ar",
            "arabic": "ar",
            "chi": "zh",
            "chinese": "zh",
            "rus": "ru",
            "russian": "ru",
        }

        return lang_map.get(lang, lang[:2] if len(lang) >= 2 else "en")


# Example usage
if __name__ == "__main__":
    # Example metadata from Internet Archive
    example = {
        "identifier": "walden00thor",
        "title": "Walden",
        "creator": "Thoreau, Henry David",
        "date": "1910",
        "subject": "Natural history; Solitude; Biography",
        "description": "Classic work by Thoreau",
        "language": "eng",
        "downloads": "150000",
    }

    translator = InternetArchiveTranslator()
    result = translator.translate(example)

    print(f"Title: {result.title}")
    print(f"Author: {result.authors[0].name if result.authors else 'Unknown'}")
    print(f"URL: {result.primary_source.url}")
    print(f"Files: {len(result.primary_source.files)}")
    print(f"Quality Score: {result.primary_source.quality_score}")
