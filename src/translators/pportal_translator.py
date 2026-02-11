"""
PPORTAL Translator

Translates PPORTAL (Portuguese Public Domain) metadata to UnifiedBookMetadata.
"""

from typing import Dict, Any, List

from src.schemas.translator import SchemaTranslator, TranslationContext
from src.schemas.unified_metadata import (
    Contributor,
    FileInfo,
    ProviderSource,
    ImageInfo,
    PopularityInfo,
)


class PPORTALTranslator(SchemaTranslator):
    """Translator for PPORTAL Portuguese literature metadata."""

    @property
    def provider_name(self) -> str:
        return "pportal"

    @property
    def source_quality(self) -> float:
        """PPORTAL is aggregated from multiple sources."""
        return 0.7

    def generate_canonical_id(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> str:
        """Generate canonical ID from obra ID."""
        obra_id = self._extract_obra_id(raw.get("download_link", ""))
        if obra_id:
            return f"pportal:{obra_id}"
        return f"pportal:{raw.get('book_id', '')}"

    def _extract_obra_id(self, download_link: str) -> str:
        """Extract co_obra ID from Domínio Público URL."""
        if not download_link:
            return ""
        import re

        match = re.search(r"co_obra=(\d+)", download_link)
        if match:
            return match.group(1)
        return ""

    def translate_core(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate core fields."""
        obra_id = self._extract_obra_id(raw.get("download_link", ""))
        book_id = obra_id if obra_id else str(raw.get("book_id", ""))

        return {
            "canonical_id": self.generate_canonical_id(raw, ctx),
            "title": raw.get("title", "Unknown"),
            "language": "pt",  # PPORTAL is Portuguese-only
            "source_url": raw.get(
                "url",
                f"http://www.dominiopublico.gov.br/pesquisa/DetalheObraForm.do?select_action=&co_obra={book_id}",
            ),
        }

    def translate_bibliographic(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate bibliographic fields."""
        authors = []
        if raw.get("author"):
            authors.extend(self.normalize_author(raw["author"]))

        # PPORTAL categories are actually source library names
        subjects = []
        if raw.get("category"):
            # Convert library source to category
            source_lib = raw["category"]
            if "[bi]" in source_lib:
                subjects.append("Biblioteca Virtual de Literatura")
            elif "[ea]" in source_lib:
                subjects.append("Edição do Autor")
            elif "[dp]" in source_lib:
                subjects.append("Domínio Público")

        return {
            "authors": authors,
            "subjects": subjects,
            "categories": [raw["category"]] if raw.get("category") else [],
            "publisher": "Domínio Público",
        }

    def translate_content(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate content fields."""
        return {
            "description": None,
            "copyright_status": "Public Domain",
            "page_count": None,
        }

    def translate_media(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate media fields."""
        files = []

        # Add download link as file
        if raw.get("download_link"):
            file_format = raw.get("format", "pdf").replace(".", "")
            files.append(
                FileInfo(
                    format=file_format,
                    url=raw["download_link"],
                    size=raw.get("file_size"),
                )
            )

        # PPORTAL doesn't provide cover images
        cover_image = None

        return {
            "files": files,
            "cover_image": cover_image,
            "thumbnail_url": None,
        }

    def translate_identifiers(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate identifier fields."""
        obra_id = self._extract_obra_id(raw.get("download_link", ""))
        book_id = obra_id if obra_id else str(raw.get("book_id", ""))

        return {
            "provider_ids": {"pportal": book_id},
        }

    def translate_enrichment(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        """Translate enrichment fields."""
        popularity = None

        return {
            "popularity": popularity,
        }
