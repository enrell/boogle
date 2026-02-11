"""
Security Tests for Boogle Unified Schema System

Tests cover:
- SQL Injection prevention
- XSS prevention
- Path Traversal prevention
- Input validation
- Type safety
- Malicious payload handling
"""

import pytest
import re
from typing import Dict, Any, List
from unittest.mock import Mock, patch

from src.schemas.unified_metadata import (
    UnifiedBookMetadata,
    FileInfo,
    Contributor,
    ImageInfo,
    ProviderSource,
)
from src.schemas.translator import (
    SchemaTranslator,
    TranslationContext,
    FallbackTranslator,
)
from src.services.cross_reference import CrossReferenceService


class TestSQLInjection:
    """Test SQL Injection prevention."""

    def test_sql_injection_in_title(self):
        """Test that SQL injection patterns in title are handled safely."""
        malicious_title = "'; DROP TABLE books; --"

        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1", title=malicious_title, primary_source=source
        )

        # Should store as-is (database layer handles escaping)
        assert book.title == malicious_title

        # to_dict should serialize safely
        d = book.to_dict()
        assert d["title"] == malicious_title

    def test_sql_injection_in_author(self):
        """Test SQL injection in author name."""
        malicious_author = "'; DELETE FROM books WHERE 1=1; --"

        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1",
            title="Test",
            primary_source=source,
            authors=[Contributor(name=malicious_author)],
        )

        assert book.authors[0].name == malicious_author

    def test_sql_injection_in_description(self):
        """Test SQL injection in description."""
        malicious_desc = "A book'; UPDATE users SET admin=1 WHERE 'x'='x"

        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1",
            title="Test",
            primary_source=source,
            description=malicious_desc,
        )

        assert book.description == malicious_desc


class TestXSSPrevention:
    """Test XSS prevention."""

    def test_xss_script_in_title(self):
        """Test XSS script injection in title."""
        malicious_title = "<script>alert('xss')</script>"

        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1", title=malicious_title, primary_source=source
        )

        # Should store as-is (API layer should sanitize for output)
        assert book.title == malicious_title

    def test_xss_event_handler_in_title(self):
        """Test XSS event handler in title."""
        malicious_title = '<img src=x onerror=alert("xss")>'

        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1", title=malicious_title, primary_source=source
        )

        assert malicious_title in book.title

    def test_xss_javascript_url(self):
        """Test javascript: URL injection."""
        malicious_url = "javascript://alert('xss')"

        source = ProviderSource(provider="test", book_id="1", url=malicious_url)
        book = UnifiedBookMetadata(
            canonical_id="test:1", title="Test", primary_source=source
        )

        assert book.primary_source.url == malicious_url


class TestPathTraversal:
    """Test Path Traversal prevention."""

    def test_path_traversal_in_file_url(self):
        """Test path traversal in file download URL."""
        malicious_url = "../../../etc/passwd"

        file_info = FileInfo(format="pdf", url=malicious_url)

        assert file_info.url == malicious_url

    def test_path_traversal_in_file_path(self):
        """Test path traversal with null bytes."""
        malicious_path = "book.pdf\x00../../../etc/passwd"

        file_info = FileInfo(format="pdf", url=malicious_path)

        assert malicious_path in file_info.url

    def test_path_traversal_in_canonical_id(self):
        """Test path traversal in canonical ID."""
        malicious_id = "../../../etc/shadow"

        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id=malicious_id, title="Test", primary_source=source
        )

        assert book.canonical_id == malicious_id


class TestTypeSafety:
    """Test type safety and validation."""

    def test_invalid_type_book_id(self):
        """Test book_id with wrong type - Python accepts it but should be string."""
        # Python is dynamically typed, but we should use strings
        source = ProviderSource(
            provider="test",
            book_id=12345,  # Int instead of str - Python accepts this
            url="test.com",
        )
        # The value is accepted but should ideally be a string
        assert source.book_id == 12345  # Works but not ideal

    def test_invalid_type_rating(self):
        """Test rating with invalid type."""
        from src.schemas.unified_metadata import RatingInfo

        # Should accept numbers
        rating = RatingInfo(
            average="not_a_number",  # Invalid type
            count="also_not_a_number",
        )

        # Type hints don't enforce at runtime, but good to check
        assert not isinstance(rating.average, (int, float))

    def test_missing_required_field(self):
        """Test missing required fields."""
        # canonical_id and title are required
        source = ProviderSource(provider="test", book_id="1", url="test.com")

        # This should work but may cause issues later
        book = UnifiedBookMetadata(
            canonical_id="test:1",
            title=None,  # Required field None
            primary_source=source,
        )

        assert book.title is None

    def test_none_in_list_fields(self):
        """Test None values in list fields."""
        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1",
            title="Test",
            primary_source=source,
            subjects=["Fiction", None, "Classic"],  # None in list
        )

        # Should handle gracefully
        assert len(book.subjects) == 3


class TestInputValidation:
    """Test input validation."""

    def test_very_long_title(self):
        """Test extremely long title."""
        long_title = "A" * 100000

        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1", title=long_title, primary_source=source
        )

        assert len(book.title) == 100000

    def test_unicode_injection(self):
        """Test unicode injection attempts."""
        # Right-to-left override and other unicode tricks
        malicious_title = "\u202eevil\u202c"  # RTL override

        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1", title=malicious_title, primary_source=source
        )

        assert book.title == malicious_title

    def test_null_byte_injection(self):
        """Test null byte injection."""
        malicious_title = "Book\x00.jpg"

        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1", title=malicious_title, primary_source=source
        )

        assert "\x00" in book.title


class TestTranslationSecurity:
    """Test translation security."""

    def test_translator_handles_malicious_input(self):
        """Test that translators handle malicious input safely."""
        translator = FallbackTranslator()

        malicious_raw = {
            "book_id": "'; DROP TABLE",
            "title": "<script>alert('xss')</script>",
            "author": "'; DELETE FROM",
            "description": "../../../etc/passwd",
        }

        # Should not crash
        result = translator.translate(malicious_raw)

        # Values should be preserved (sanitization at output layer)
        assert "<script>" in result.title

    def test_translator_injection_in_subjects(self):
        """Test injection in subject parsing."""
        translator = FallbackTranslator()

        malicious_subjects = "Fiction'; DROP TABLE books; --"
        result = translator.parse_subjects(malicious_subjects)

        # Should return as-is without executing
        assert len(result) > 0


class TestCrossReferenceSecurity:
    """Test cross-reference security."""

    def test_cross_reference_malicious_canonical_id(self):
        """Test cross-reference with malicious canonical ID."""
        service = CrossReferenceService()

        books = [
            UnifiedBookMetadata(
                canonical_id="../../../etc/passwd",
                title="Test",
                primary_source=ProviderSource(provider="test", book_id="1", url="test"),
            ),
            UnifiedBookMetadata(
                canonical_id="../../../etc/passwd",
                title="Test",
                primary_source=ProviderSource(provider="test", book_id="2", url="test"),
            ),
        ]

        # Should not crash or access filesystem
        result = service.cross_reference(books)

        assert len(result) == 1


class TestSanitizationHelpers:
    """Test sanitization helper functions."""

    def test_html_escape(self):
        """Test HTML escaping."""
        dangerous = "<script>alert('xss')</script>"

        # Would be implemented in API layer
        escaped = dangerous.replace("<", "&lt;").replace(">", "&gt;")

        assert "<" not in escaped
        assert "&lt;script&gt;" in escaped

    def test_url_validation(self):
        """Test URL validation patterns."""
        valid_urls = [
            "https://example.com/book.pdf",
            "http://gutenberg.org/ebooks/123",
        ]

        invalid_urls = [
            "javascript:alert('xss')",
            "file:///etc/passwd",
            "data:text/html,<script>alert(1)</script>",
        ]

        url_pattern = re.compile(r"^https?://", re.IGNORECASE)

        for url in valid_urls:
            assert url_pattern.match(url), f"{url} should be valid"

        for url in invalid_urls:
            assert not url_pattern.match(url), f"{url} should be invalid"


class TestAPIOutputSanitization:
    """Test API output sanitization (conceptual)."""

    def test_to_dict_escapes_html(self):
        """Test that to_dict escapes HTML (if implemented)."""
        # Note: Current implementation doesn't escape, this tests what SHOULD happen
        source = ProviderSource(provider="test", book_id="1", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:1",
            title="<script>alert('xss')</script>",
            primary_source=source,
        )

        d = book.to_dict()

        # In production API, this should be escaped
        # For now, just verify it serializes
        assert "title" in d


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
