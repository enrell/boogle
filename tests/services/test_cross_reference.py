"""
Comprehensive tests for Cross-Reference Service.

Tests cover duplicate detection, metadata merging, and source ranking.
"""

import pytest
from typing import List

from src.services.cross_reference import CrossReferenceService, CrossReferenceConfig
from src.schemas.unified_metadata import (
    UnifiedBookMetadata,
    ProviderSource,
    Contributor,
    RatingInfo,
    PopularityInfo,
)


class TestCrossReferenceService:
    """Test CrossReferenceService functionality."""

    @pytest.fixture
    def service(self):
        return CrossReferenceService()

    def create_book(
        self,
        title: str = "Test Book",
        author: str = "Test Author",
        provider: str = "test",
        quality: float = 1.0,
    ) -> UnifiedBookMetadata:
        """Helper to create a test book."""
        source = ProviderSource(
            provider=provider,
            book_id="123",
            url=f"https://{provider}.com/123",
            quality_score=quality,
        )
        return UnifiedBookMetadata(
            canonical_id=f"{provider}:123",
            title=title,
            primary_source=source,
            all_sources=[source],
            authors=[Contributor(name=author)] if author else [],
        )

    def test_normalize_title(self, service):
        """Test title normalization."""
        assert service.normalize_title("The Great Gatsby") == "great gatsby"
        assert service.normalize_title("A Tale of Two Cities") == "tale of two cities"
        assert service.normalize_title("Dom Casmurro") == "dom casmurro"
        assert service.normalize_title("") == ""

    def test_normalize_author(self, service):
        """Test author normalization."""
        assert service.normalize_author("Twain, Mark") == "mark twain"
        assert service.normalize_author("Mark Twain") == "mark twain"
        assert service.normalize_author("Dr. John Doe") == "john doe"
        assert service.normalize_author("") == ""

    def test_calculate_title_similarity_exact(self, service):
        """Test title similarity - exact match."""
        assert (
            service.calculate_title_similarity(
                "Pride and Prejudice", "Pride and Prejudice"
            )
            == 1.0
        )

    def test_calculate_title_similarity_contains(self, service):
        """Test title similarity - one contains other."""
        assert (
            service.calculate_title_similarity(
                "Pride and Prejudice", "Pride and Prejudice: A Novel"
            )
            == 0.95
        )

    def test_calculate_title_similarity_different(self, service):
        """Test title similarity - completely different."""
        similarity = service.calculate_title_similarity(
            "Pride and Prejudice", "War and Peace"
        )
        assert 0.0 < similarity < 0.5

    def test_authors_match_same(self, service):
        """Test author matching - same author."""
        book1 = self.create_book(author="Jane Austen")
        book2 = self.create_book(author="Jane Austen")

        assert service.authors_match(book1.authors, book2.authors)

    def test_authors_match_different_format(self, service):
        """Test author matching - different format."""
        book1 = UnifiedBookMetadata(
            canonical_id="test:1",
            title="Test",
            primary_source=ProviderSource(provider="test", book_id="1", url="test"),
            authors=[Contributor(name="Austen, Jane")],
        )
        book2 = UnifiedBookMetadata(
            canonical_id="test:2",
            title="Test",
            primary_source=ProviderSource(provider="test", book_id="1", url="test"),
            authors=[Contributor(name="Jane Austen")],
        )

        assert service.authors_match(book1.authors, book2.authors)

    def test_authors_match_empty(self, service):
        """Test author matching - one empty."""
        book1 = self.create_book(author="Jane Austen")
        book2 = self.create_book(author="")

        # Should return True (can't verify, assume match)
        assert service.authors_match(book1.authors, book2.authors)

    def test_is_duplicate_exact_match(self, service):
        """Test duplicate detection - exact match."""
        book1 = self.create_book(title="Test Book", author="Author")
        book2 = self.create_book(title="Test Book", author="Author")

        assert service.is_duplicate(book1, book2)

    def test_is_duplicate_similar_titles(self, service):
        """Test duplicate detection - similar titles."""
        book1 = self.create_book(title="The Great Gatsby", author="F. Scott Fitzgerald")
        book2 = self.create_book(
            title="Great Gatsby, The", author="F. Scott Fitzgerald"
        )

        assert service.is_duplicate(book1, book2)

    def test_is_duplicate_different_authors(self, service):
        """Test duplicate detection - different authors."""
        book1 = UnifiedBookMetadata(
            canonical_id="test:a",
            title="Test",
            primary_source=ProviderSource(provider="test", book_id="1", url="test"),
            authors=[Contributor(name="Author A")],
        )
        book2 = UnifiedBookMetadata(
            canonical_id="test:b",
            title="Test",
            primary_source=ProviderSource(provider="test", book_id="2", url="test"),
            authors=[Contributor(name="Author B")],
        )

        # Different authors should not be duplicates
        assert not service.is_duplicate(book1, book2)

    def test_is_duplicate_different_titles(self, service):
        """Test duplicate detection - completely different titles."""
        book1 = UnifiedBookMetadata(
            canonical_id="test:pride",
            title="Pride and Prejudice",
            primary_source=ProviderSource(provider="test", book_id="1", url="test"),
        )
        book2 = UnifiedBookMetadata(
            canonical_id="test:war",
            title="War and Peace",
            primary_source=ProviderSource(provider="test", book_id="2", url="test"),
        )

        assert not service.is_duplicate(book1, book2)

    def test_merge_two_books(self, service):
        """Test merging two books."""
        gutenberg = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Dom Casmurro",
            subtitle=None,
            primary_source=ProviderSource(
                provider="gutenberg",
                book_id="123",
                url="gutenberg.org",
                quality_score=1.0,
            ),
            all_sources=[],
            authors=[Contributor(name="Machado de Assis")],
            description=None,
            cover_image=None,
        )

        pportal = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Dom Casmurro",
            subtitle="Memórias Póstumas de Brás Cubas",
            primary_source=ProviderSource(
                provider="pportal", book_id="456", url="pportal.gov", quality_score=0.7
            ),
            all_sources=[],
            authors=[Contributor(name="Machado de Assis")],
            description="A classic Brazilian novel",
            cover_image=None,
        )

        merged = service.merge_books([gutenberg, pportal])

        # Should pick higher quality as primary
        assert merged.primary_source.provider == "gutenberg"

        # Should have both sources
        assert merged.source_count == 2

        # Should merge metadata
        assert merged.subtitle == "Memórias Póstumas de Brás Cubas"
        assert merged.description == "A classic Brazilian novel"

    def test_cross_reference_no_duplicates(self, service):
        """Test cross-reference with no duplicates."""
        books = [
            self.create_book(title="Book A", author="Author A", provider="gutenberg"),
            self.create_book(title="Book B", author="Author B", provider="openlibrary"),
        ]

        result = service.cross_reference(books)

        assert len(result) == 2

    def test_cross_reference_with_duplicates(self, service):
        """Test cross-reference with duplicates."""
        books = [
            self.create_book(
                title="Dom Casmurro", author="Machado de Assis", provider="gutenberg"
            ),
            self.create_book(
                title="Dom Casmurro", author="Machado de Assis", provider="pportal"
            ),
            self.create_book(
                title="Other Book", author="Other Author", provider="openlibrary"
            ),
        ]

        # Make first two have same canonical_id
        books[0].canonical_id = "same:123"
        books[1].canonical_id = "same:123"

        result = service.cross_reference(books)

        # Should merge first two, keep third separate
        assert len(result) == 2

        # Check merged book has multiple sources
        merged_book = next(b for b in result if b.canonical_id == "same:123")
        assert merged_book.source_count == 2

    def test_cross_reference_empty_list(self, service):
        """Test cross-reference with empty list."""
        result = service.cross_reference([])
        assert len(result) == 0

    def test_find_related_books(self, service):
        """Test finding related books."""
        # Create books with same author to ensure they're related
        books = [
            UnifiedBookMetadata(
                canonical_id="test:1",
                title="Pride and Prejudice",
                primary_source=ProviderSource(
                    provider="gutenberg", book_id="1", url="test"
                ),
                authors=[Contributor(name="Jane Austen")],
                subjects=["Romance", "Classic"],
            ),
            UnifiedBookMetadata(
                canonical_id="test:2",
                title="Emma",
                primary_source=ProviderSource(
                    provider="gutenberg", book_id="2", url="test"
                ),
                authors=[Contributor(name="Jane Austen")],  # Same author
                subjects=["Romance", "Classic"],  # Same subjects
            ),
            UnifiedBookMetadata(
                canonical_id="test:3",
                title="War and Peace",
                primary_source=ProviderSource(
                    provider="gutenberg", book_id="3", url="test"
                ),
                authors=[Contributor(name="Leo Tolstoy")],
                subjects=["War", "History"],
            ),
        ]

        related = service.find_related(books[0], books[1:])

        # Should find Emma as related (same author, same subjects)
        assert len(related) > 0
        # The first related book should be Emma (by Jane Austen)
        assert related[0][0].title == "Emma"
        assert related[0][1] > 0.3  # Should have decent similarity score

    def test_edge_case_books_with_no_metadata(self, service):
        """Test handling books with minimal metadata."""
        book = UnifiedBookMetadata(
            canonical_id="test:123",
            title="Unknown",
            primary_source=ProviderSource(provider="test", book_id="123", url="test"),
            authors=[],
        )

        # Should not crash
        completeness = book.calculate_completeness()
        assert completeness >= 0.0

    def test_edge_case_very_long_titles(self, service):
        """Test handling very long titles."""
        long_title = "A" * 5000
        book1 = self.create_book(title=long_title)
        book2 = self.create_book(title=long_title)

        similarity = service.calculate_title_similarity(long_title, long_title)
        assert similarity == 1.0

    def test_edge_case_unicode_characters(self, service):
        """Test handling unicode characters."""
        book1 = self.create_book(title="Memórias Póstumas", author="Machado de Assis")
        book2 = self.create_book(title="Memórias Póstumas", author="Machado de Assis")

        assert service.is_duplicate(book1, book2)

    def test_security_malicious_canonical_id(self, service):
        """Test handling malicious canonical IDs."""
        malicious_id = "../../../etc/passwd"

        book = UnifiedBookMetadata(
            canonical_id=malicious_id,
            title="Test",
            primary_source=ProviderSource(provider="test", book_id="1", url="test"),
        )

        # Should store as-is (path validation at file system layer)
        assert book.canonical_id == malicious_id


class TestCrossReferenceConfig:
    """Test CrossReferenceConfig."""

    def test_default_config(self):
        """Test default configuration."""
        config = CrossReferenceConfig()

        assert config.title_similarity_threshold == 0.85
        assert config.author_match_required == True
        assert "gutenberg" in config.prefer_source_order

    def test_custom_config(self):
        """Test custom configuration."""
        config = CrossReferenceConfig(
            title_similarity_threshold=0.95, author_match_required=False
        )

        assert config.title_similarity_threshold == 0.95
        assert config.author_match_required == False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
