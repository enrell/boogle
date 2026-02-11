"""
Comprehensive tests for UnifiedBookMetadata schema.

Tests cover:
- Dataclass initialization
- Field validation
- Method correctness
- Edge cases
- Security (malicious input handling)
"""

import pytest
from datetime import datetime
from src.schemas.unified_metadata import (
    UnifiedBookMetadata,
    FileInfo,
    Contributor,
    ImageInfo,
    RatingInfo,
    PopularityInfo,
    ProviderSource,
    SearchSignals,
)


class TestFileInfo:
    """Test FileInfo dataclass."""

    def test_basic_creation(self):
        """Test basic FileInfo creation."""
        file_info = FileInfo(
            format="pdf", url="https://example.com/book.pdf", size="1.5 MB"
        )
        assert file_info.format == "pdf"
        assert file_info.url == "https://example.com/book.pdf"
        assert file_info.size == "1.5 MB"
        assert file_info.checksum is None

    def test_optional_checksum(self):
        """Test FileInfo with checksum."""
        file_info = FileInfo(
            format="epub", url="https://example.com/book.epub", checksum="abc123"
        )
        assert file_info.checksum == "abc123"

    def test_empty_url_raises(self):
        """Test that empty URL is handled gracefully."""
        file_info = FileInfo(format="pdf", url="")
        assert file_info.url == ""

    def test_malicious_url_in_field(self):
        """Test that URLs with malicious content don't break."""
        malicious_url = "javascript://alert('xss')"
        file_info = FileInfo(format="pdf", url=malicious_url)
        assert file_info.url == malicious_url  # We don't sanitize here, API layer does


class TestContributor:
    """Test Contributor dataclass."""

    def test_author_creation(self):
        """Test author creation with default role."""
        author = Contributor(name="Jane Austen")
        assert author.name == "Jane Austen"
        assert author.role == "author"

    def test_illustrator_creation(self):
        """Test illustrator creation."""
        illustrator = Contributor(name="John Smith", role="illustrator")
        assert illustrator.name == "John Smith"
        assert illustrator.role == "illustrator"

    def test_empty_name(self):
        """Test empty name handling."""
        contributor = Contributor(name="")
        assert contributor.name == ""


class TestImageInfo:
    """Test ImageInfo dataclass."""

    def test_basic_image(self):
        """Test basic ImageInfo."""
        img = ImageInfo(url="https://example.com/cover.jpg")
        assert img.url == "https://example.com/cover.jpg"
        assert img.width is None

    def test_full_image_metadata(self):
        """Test ImageInfo with all fields."""
        img = ImageInfo(
            url="https://example.com/cover.jpg", width=200, height=300, format="jpg"
        )
        assert img.width == 200
        assert img.height == 300
        assert img.format == "jpg"

    def test_zero_dimensions(self):
        """Test ImageInfo with zero dimensions."""
        img = ImageInfo(url="test.jpg", width=0, height=0)
        assert img.width == 0
        assert img.height == 0


class TestRatingInfo:
    """Test RatingInfo dataclass."""

    def test_basic_rating(self):
        """Test rating with average and count."""
        rating = RatingInfo(average=4.5, count=100)
        assert rating.average == 4.5
        assert rating.count == 100

    def test_out_of_bounds_rating(self):
        """Test that out-of-bounds ratings don't break (validation at API layer)."""
        rating = RatingInfo(average=10.0, count=-5)
        assert rating.average == 10.0  # Not validated here
        assert rating.count == -5


class TestPopularityInfo:
    """Test PopularityInfo dataclass."""

    def test_all_fields(self):
        """Test PopularityInfo with all fields."""
        pop = PopularityInfo(
            downloads=1000,
            views=5000,
            want_to_read=200,
            currently_reading=50,
            shelves=300,
        )
        assert pop.downloads == 1000
        assert pop.views == 5000
        assert pop.want_to_read == 200

    def test_partial_fields(self):
        """Test PopularityInfo with partial fields."""
        pop = PopularityInfo(downloads=100)
        assert pop.downloads == 100
        assert pop.views is None


class TestProviderSource:
    """Test ProviderSource dataclass."""

    def test_basic_source(self):
        """Test basic ProviderSource."""
        source = ProviderSource(
            provider="gutenberg",
            book_id="12345",
            url="https://gutenberg.org/ebooks/12345",
        )
        assert source.provider == "gutenberg"
        assert source.quality_score == 1.0  # Default

    def test_source_with_files(self):
        """Test ProviderSource with files."""
        files = [FileInfo(format="pdf", url="test.pdf")]
        source = ProviderSource(
            provider="gutenberg",
            book_id="12345",
            url="test",
            files=files,
            quality_score=0.9,
        )
        assert len(source.files) == 1
        assert source.quality_score == 0.9


class TestUnifiedBookMetadata:
    """Test UnifiedBookMetadata main dataclass."""

    def test_minimal_creation(self):
        """Test creation with only required fields."""
        source = ProviderSource(
            provider="gutenberg", book_id="12345", url="https://example.com"
        )
        book = UnifiedBookMetadata(
            canonical_id="gutenberg:12345", title="Test Book", primary_source=source
        )
        assert book.canonical_id == "gutenberg:12345"
        assert book.title == "Test Book"
        assert book.authors == []  # Default

    def test_full_metadata(self):
        """Test creation with all fields."""
        source = ProviderSource(
            provider="gutenberg", book_id="12345", url="https://example.com"
        )
        book = UnifiedBookMetadata(
            canonical_id="gutenberg:12345",
            title="Pride and Prejudice",
            subtitle="A Novel",
            primary_source=source,
            all_sources=[source],
            authors=[Contributor(name="Jane Austen")],
            language="en",
            subjects=["Fiction", "Romance"],
            publication_year=1813,
            description="A classic novel",
            page_count=432,
        )
        assert book.publication_year == 1813
        assert len(book.authors) == 1

    def test_to_dict_method(self):
        """Test to_dict conversion."""
        source = ProviderSource(
            provider="gutenberg", book_id="12345", url="https://example.com"
        )
        book = UnifiedBookMetadata(
            canonical_id="gutenberg:12345",
            title="Test Book",
            primary_source=source,
            authors=[Contributor(name="Author")],
        )

        d = book.to_dict()
        assert d["canonical_id"] == "gutenberg:12345"
        assert d["title"] == "Test Book"
        assert d["authors"] == [{"name": "Author", "role": "author"}]

    def test_get_best_source(self):
        """Test get_best_source method."""
        gutenberg = ProviderSource(
            provider="gutenberg",
            book_id="12345",
            url="gutenberg.org",
            quality_score=1.0,
        )
        pportal = ProviderSource(
            provider="pportal", book_id="5678", url="pportal.gov", quality_score=0.7
        )

        book = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Test",
            primary_source=pportal,  # Lower quality
            all_sources=[pportal, gutenberg],
        )

        # Should return gutenberg (highest quality)
        best = book.get_best_source()
        assert best.provider == "gutenberg"

    def test_get_source(self):
        """Test get_source method."""
        gutenberg = ProviderSource(
            provider="gutenberg", book_id="12345", url="gutenberg.org"
        )

        book = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Test",
            primary_source=gutenberg,
            all_sources=[gutenberg],
        )

        source = book.get_source("gutenberg")
        assert source is not None
        assert source.provider == "gutenberg"

        # Non-existent source
        assert book.get_source("nonexistent") is None

    def test_add_source(self):
        """Test add_source method."""
        primary = ProviderSource(
            provider="pportal", book_id="123", url="pportal.gov", quality_score=0.7
        )

        book = UnifiedBookMetadata(
            canonical_id="test:123", title="Test", primary_source=primary
        )

        # Add better source
        gutenberg = ProviderSource(
            provider="gutenberg",
            book_id="12345",
            url="gutenberg.org",
            quality_score=1.0,
        )

        book.add_source(gutenberg)

        assert book.source_count == 2
        assert (
            book.primary_source.provider == "gutenberg"
        )  # Higher quality becomes primary
        assert "gutenberg" in book.provider_ids

    def test_calculate_completeness_empty(self):
        """Test completeness calculation with empty metadata."""
        source = ProviderSource(provider="test", book_id="123", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:123", title="Test", primary_source=source
        )

        completeness = book.calculate_completeness()
        assert 0.0 <= completeness <= 1.0

    def test_calculate_completeness_full(self):
        """Test completeness with all fields."""
        source = ProviderSource(provider="test", book_id="123", url="test.com")
        rating = RatingInfo(average=4.5, count=100)
        cover = ImageInfo(url="cover.jpg")

        book = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Test",
            subtitle="Subtitle",
            primary_source=source,
            authors=[Contributor(name="Author")],
            language="en",
            subjects=["Fiction"],
            publication_year=2020,
            description="Description",
            page_count=200,
            cover_image=cover,
            rating=rating,
        )

        completeness = book.calculate_completeness()
        assert completeness > 0.8  # Should be very complete

    def test_edge_case_special_characters_in_title(self):
        """Test handling of special characters in title."""
        source = ProviderSource(provider="test", book_id="123", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Book with <script>alert('xss')</script>",
            primary_source=source,
        )

        # Should store as-is (sanitization at API layer)
        assert "<script>" in book.title

    def test_edge_case_unicode_in_title(self):
        """Test handling of unicode in title."""
        source = ProviderSource(provider="test", book_id="123", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Dom Casmurro - Memórias Póstumas de Brás Cubas",
            primary_source=source,
        )

        assert "Dom Casmurro" in book.title

    def test_edge_case_very_long_title(self):
        """Test handling of very long title."""
        source = ProviderSource(provider="test", book_id="123", url="test.com")
        long_title = "A" * 10000
        book = UnifiedBookMetadata(
            canonical_id="test:123", title=long_title, primary_source=source
        )

        assert len(book.title) == 10000

    def test_edge_case_empty_sources_list(self):
        """Test with empty all_sources list."""
        primary = ProviderSource(provider="test", book_id="123", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Test",
            primary_source=primary,
            all_sources=[],
        )

        # get_best_source should return primary when list is empty
        best = book.get_best_source()
        assert best.provider == "test"

    def test_edge_case_none_values_in_list_fields(self):
        """Test handling of None values in list fields."""
        source = ProviderSource(provider="test", book_id="123", url="test.com")
        book = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Test",
            primary_source=source,
            subjects=["Fiction", None, "Romance"],  # None in list
        )

        assert "Fiction" in book.subjects
        assert None in book.subjects

    def test_security_sql_injection_in_title(self):
        """Test that SQL injection patterns in metadata don't cause issues."""
        source = ProviderSource(provider="test", book_id="123", url="test.com")
        malicious_title = "'; DROP TABLE books; --"
        book = UnifiedBookMetadata(
            canonical_id="test:123", title=malicious_title, primary_source=source
        )

        # Should store as-is (database layer handles escaping)
        assert book.title == malicious_title

    def test_security_path_traversal_in_file_url(self):
        """Test path traversal in file URLs."""
        file_info = FileInfo(format="pdf", url="../../../etc/passwd")
        # Should store as-is (validation at download layer)
        assert "../../../etc/passwd" in file_info.url


class TestSearchSignals:
    """Test SearchSignals dataclass."""

    def test_default_signals(self):
        """Test default signal values."""
        signals = SearchSignals()
        assert signals.metadata_completeness == 0.0
        assert signals.source_quality_score == 0.8
        assert signals.relevance_boost == 1.0

    def test_custom_signals(self):
        """Test custom signal values."""
        signals = SearchSignals(
            metadata_completeness=0.9, source_quality_score=1.0, relevance_boost=1.5
        )
        assert signals.metadata_completeness == 0.9


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
