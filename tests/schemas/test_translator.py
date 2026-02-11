"""
Comprehensive tests for SchemaTranslator base class.

Tests cover all translation phases, validation, and edge cases.
"""

import pytest
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from src.schemas.translator import (
    SchemaTranslator,
    TranslationContext,
    FallbackTranslator,
)
from src.schemas.unified_metadata import (
    UnifiedBookMetadata,
    FileInfo,
    Contributor,
    ProviderSource,
)


class MockTranslator(SchemaTranslator):
    """Mock translator for testing."""

    @property
    def provider_name(self) -> str:
        return "mock"

    @property
    def source_quality(self) -> float:
        return 0.8

    def translate_core(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {
            "canonical_id": f"mock:{raw.get('book_id', 'unknown')}",
            "title": raw.get("title", "Unknown"),
            "language": raw.get("language", "en"),
            "source_url": f"https://mock.com/{raw.get('book_id', '')}",
        }

    def translate_bibliographic(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {
            "authors": [Contributor(name=raw.get("author", "Unknown"))]
            if raw.get("author")
            else [],
            "subjects": raw.get("subjects", []),
            "publication_year": raw.get("year"),
        }

    def translate_content(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {
            "description": raw.get("description"),
            "copyright_status": raw.get("copyright", "Unknown"),
        }

    def translate_media(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {
            "cover_image": None,
            "files": [],
        }

    def translate_identifiers(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {
            "provider_ids": {"mock": str(raw.get("book_id", ""))},
        }

    def translate_enrichment(
        self, raw: Dict[str, Any], ctx: TranslationContext
    ) -> Dict[str, Any]:
        return {}


class TestSchemaTranslator:
    """Test base SchemaTranslator functionality."""

    @pytest.fixture
    def translator(self):
        return MockTranslator()

    def test_translate_full_book(self, translator):
        """Test full translation of a book."""
        raw = {
            "book_id": "12345",
            "title": "Test Book",
            "author": "John Doe",
            "language": "en",
            "subjects": ["Fiction", "Test"],
            "year": 2020,
            "description": "A test book",
            "copyright": "Public Domain",
        }

        result = translator.translate(raw)

        assert isinstance(result, UnifiedBookMetadata)
        assert result.canonical_id == "mock:12345"
        assert result.title == "Test Book"
        assert len(result.authors) == 1
        assert result.authors[0].name == "John Doe"
        assert result.language == "en"
        assert result.publication_year == 2020

    def test_generate_canonical_id(self, translator):
        """Test canonical ID generation."""
        raw = {"book_id": "12345", "title": "Test Book", "author": "John Doe"}
        ctx = TranslationContext(provider_name="test", raw_metadata=raw)

        canonical = translator.generate_canonical_id(raw, ctx)
        assert isinstance(canonical, str)
        assert len(canonical) == 16  # MD5 hash truncated

    def test_generate_canonical_id_with_title_and_author(self, translator):
        """Test canonical ID with title and author."""
        raw = {"title": "Test", "author": "Author", "book_id": "1"}
        ctx = TranslationContext(provider_name="test", raw_metadata=raw)

        id1 = translator.generate_canonical_id(raw, ctx)

        # Same book should generate same ID
        id2 = translator.generate_canonical_id(raw, ctx)
        assert id1 == id2

    def test_calculate_completeness(self, translator):
        """Test completeness calculation."""
        # Empty metadata
        empty = {}
        completeness = translator.calculate_completeness(empty)
        assert completeness == 0.0

        # Full metadata
        full = {
            "title": "Test",
            "authors": [Contributor(name="Author")],
            "language": "en",
            "subjects": ["Fiction"],
            "publication_year": 2020,
            "description": "Desc",
            "cover_image": "cover.jpg",
            "page_count": 200,
            "isbn_13": "1234567890123",
            "rating": {"average": 4.5, "count": 10},
        }
        completeness = translator.calculate_completeness(full)
        assert completeness > 0.8

    def test_normalize_author_single(self, translator):
        """Test author normalization - single name."""
        authors = translator.normalize_author("John Doe")
        assert len(authors) == 1
        assert authors[0].name == "John Doe"
        assert authors[0].role == "author"

    def test_normalize_author_last_first(self, translator):
        """Test author normalization - Last, First format."""
        authors = translator.normalize_author("Doe, John")
        assert len(authors) == 1
        assert "John Doe" in authors[0].name

    def test_normalize_author_unknown(self, translator):
        """Test author normalization - unknown."""
        authors = translator.normalize_author("Unknown")
        assert len(authors) == 0  # Should filter out "Unknown"

    def test_normalize_author_empty(self, translator):
        """Test author normalization - empty."""
        authors = translator.normalize_author("")
        assert len(authors) == 0

    def test_parse_subjects_comma_delimited(self, translator):
        """Test subject parsing - comma delimited."""
        subjects = translator.parse_subjects("Fiction, Romance, Classic")
        assert len(subjects) == 3
        assert "Fiction" in subjects

    def test_parse_subjects_empty(self, translator):
        """Test subject parsing - empty."""
        subjects = translator.parse_subjects("")
        assert len(subjects) == 0

    def test_parse_subjects_with_short_words(self, translator):
        """Test subject parsing - filters short words."""
        subjects = translator.parse_subjects("A, An, The, Fiction")
        assert "Fiction" in subjects
        assert "A" not in subjects  # Too short

    def test_extract_year_from_string(self, translator):
        """Test year extraction from string."""
        assert translator.extract_year("Published in 2020") == 2020
        assert translator.extract_year("1999-01-01") == 1999
        assert translator.extract_year("No year here") is None

    def test_edge_case_unicode_in_title(self, translator):
        """Test translation with unicode characters."""
        raw = {
            "book_id": "1",
            "title": "Dom Casmurro - Memórias Póstumas",
            "author": "Machado de Assis",
        }

        result = translator.translate(raw)
        assert "Memórias Póstumas" in result.title

    def test_edge_case_very_long_title(self, translator):
        """Test translation with very long title."""
        raw = {
            "book_id": "1",
            "title": "A" * 10000,
        }

        result = translator.translate(raw)
        assert len(result.title) == 10000

    def test_edge_case_special_characters_in_metadata(self, translator):
        """Test translation with special characters."""
        raw = {
            "book_id": "1",
            "title": 'Book with <script> & "quotes"',
            "author": "Author O'Brien",
        }

        result = translator.translate(raw)
        assert result.title == 'Book with <script> & "quotes"'
        assert "O'Brien" in result.authors[0].name

    def test_edge_case_none_book_id(self, translator):
        """Test translation with None book_id."""
        raw = {
            "book_id": None,
            "title": "Test",
        }

        result = translator.translate(raw)
        assert result.canonical_id == "mock:None"

    def test_security_sql_injection_in_title(self, translator):
        """Test SQL injection patterns are stored safely."""
        raw = {
            "book_id": "1",
            "title": "'; DROP TABLE books; --",
        }

        result = translator.translate(raw)
        # Should store as-is (database layer handles escaping)
        assert "'; DROP TABLE books; --" == result.title

    def test_security_xss_in_description(self, translator):
        """Test XSS patterns in description."""
        raw = {
            "book_id": "1",
            "title": "Test",
            "description": "<script>alert('xss')</script>",
        }

        result = translator.translate(raw)
        assert "<script>" in result.description


class TestFallbackTranslator:
    """Test FallbackTranslator."""

    @pytest.fixture
    def fallback(self):
        return FallbackTranslator()

    def test_fallback_basic_translation(self, fallback):
        """Test fallback translation."""
        raw = {
            "book_id": "123",
            "title": "Test Book",
            "author": "Author",
            "language": "en",
        }

        result = fallback.translate(raw)

        assert result.title == "Test Book"
        assert result.canonical_id is not None

    def test_fallback_unknown_provider(self, fallback):
        """Test fallback handles unknown provider."""
        raw = {"book_id": "1", "title": "Test"}

        result = fallback.translate(raw)
        assert result.primary_source.provider == "fallback"
        assert result.primary_source.quality_score == 0.5

    def test_fallback_source_quality(self, fallback):
        """Test fallback source quality."""
        assert fallback.source_quality == 0.5


class TestTranslationContext:
    """Test TranslationContext dataclass."""

    def test_context_creation(self):
        """Test TranslationContext creation."""
        ctx = TranslationContext(
            provider_name="test", raw_metadata={"id": 1}, confidence_threshold=0.9
        )

        assert ctx.provider_name == "test"
        assert ctx.confidence_threshold == 0.9

    def test_context_defaults(self):
        """Test TranslationContext defaults."""
        ctx = TranslationContext(provider_name="test", raw_metadata={})

        assert ctx.confidence_threshold == 0.7


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
