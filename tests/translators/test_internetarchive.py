"""
Tests for Internet Archive Translator

Demonstrates how new providers integrate with the system.
"""

import pytest
from src.translators.internetarchive_translator import InternetArchiveTranslator
from src.schemas.translator_registry import TranslatorRegistry


class TestInternetArchiveTranslator:
    """Test Internet Archive translator."""

    @pytest.fixture
    def translator(self):
        return InternetArchiveTranslator()

    def test_provider_name(self, translator):
        """Test provider name."""
        assert translator.provider_name == "internetarchive"

    def test_source_quality(self, translator):
        """Test quality score."""
        assert translator.source_quality == 0.85

    def test_translate_basic_book(self, translator):
        """Test basic translation."""
        raw = {
            "identifier": "walden00thor",
            "title": "Walden",
            "creator": "Thoreau, Henry David",
            "date": "1910",
            "subject": "Natural history; Solitude",
            "language": "eng",
            "downloads": "150000",
        }

        result = translator.translate(raw)

        assert result.title == "Walden"
        assert result.authors[0].name == "Thoreau, Henry David"
        assert result.language == "en"
        assert result.publication_year == 1910
        assert "Natural history" in result.subjects

    def test_translate_with_files(self, translator):
        """Test that files are created."""
        raw = {
            "identifier": "test123",
            "title": "Test",
        }

        result = translator.translate(raw)

        # Should have PDF, EPUB, TXT, MOBI
        assert len(result.primary_source.files) == 4

        # Check URL format
        pdf_file = next(f for f in result.primary_source.files if f.format == "pdf")
        assert "archive.org/download" in pdf_file.url

    def test_translate_multiple_authors(self, translator):
        """Test handling multiple authors."""
        raw = {
            "identifier": "test123",
            "title": "Test",
            "creator": "Author One; Author Two",
        }

        result = translator.translate(raw)

        assert len(result.authors) == 2

    def test_translate_public_domain(self, translator):
        """Test public domain detection."""
        raw = {
            "identifier": "test123",
            "title": "Test",
            "licenseurl": "https://creativecommons.org/publicdomain/",
        }

        result = translator.translate(raw)

        assert result.copyright_status == "Public Domain"

    def test_normalize_language(self, translator):
        """Test language normalization."""
        assert translator._normalize_language("eng") == "en"
        assert translator._normalize_language("English") == "en"
        assert translator._normalize_language("spa") == "es"
        assert translator._normalize_language(None) == "en"


class TestInternetArchiveIntegration:
    """Test integration with registry."""

    def test_registry_has_internetarchive(self):
        """Test that Internet Archive is in registry."""
        # Import to trigger registration
        from src.translators.internetarchive_translator import InternetArchiveTranslator

        # Check if in registry
        translator = TranslatorRegistry.get("internetarchive")

        assert translator is not None
        assert translator.provider_name == "internetarchive"

    def test_translation_via_registry(self):
        """Test translation through registry."""
        raw = {
            "identifier": "test123",
            "title": "Test Book",
            "creator": "Test Author",
        }

        result = TranslatorRegistry.translate("internetarchive", raw)

        assert result is not None
        assert result["title"] == "Test Book"
        assert "internetarchive" in result["provider_ids"]
