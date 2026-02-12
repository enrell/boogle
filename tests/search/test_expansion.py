"""
Tests for query expansion module.
"""

import pytest
from src.search.expansion import QueryExpander, BookQueryExpander


class TestQueryExpander:
    """Test basic query expansion functionality."""

    def test_init(self):
        """Test initialization."""
        expander = QueryExpander(max_expansions=5, min_word_length=3)
        assert expander.max_expansions == 5
        assert expander.min_word_length == 3

    def test_expand_english_stopwords(self):
        """Test that English stopwords are filtered."""
        expander = QueryExpander()

        # Stopwords should return empty set
        assert expander.expand_word("the", "en") == set()
        assert expander.expand_word("a", "en") == set()
        assert expander.expand_word("is", "en") == set()

    def test_expand_english_content_words(self):
        """Test expansion of English content words."""
        expander = QueryExpander()

        # Content words should have expansions
        synonyms = expander.expand_word("book", "en")
        assert len(synonyms) > 0

        # Check that at least some expected synonyms are present
        expected = {"volume", "novel", "publication"}
        assert any(s in synonyms for s in expected)

    def test_expand_short_words(self):
        """Test that short words are not expanded."""
        expander = QueryExpander(min_word_length=4)

        # Short words should not be expanded
        assert expander.expand_word("cat", "en") == set()
        assert expander.expand_word("dog", "en") == set()

    def test_expand_query(self):
        """Test full query expansion."""
        expander = QueryExpander()

        query = "author writes book"
        expanded, info = expander.expand_query(query, "en")

        # Should have some expansions
        assert len(info) > 0

        # Expanded query should contain original
        assert "author" in expanded
        assert "book" in expanded

        # Should have OR clauses for expansions
        assert " OR " in expanded

    def test_expand_query_with_stopwords(self):
        """Test that stopwords are not expanded in queries."""
        expander = QueryExpander()

        query = "the book"
        expanded, info = expander.expand_query(query, "en")

        # "the" is a stopword and should not be expanded
        assert "the" not in info

        # "book" should be expanded
        assert "book" in info


class TestBookQueryExpander:
    """Test book-specific query expansion."""

    def test_book_synonyms_en(self):
        """Test English book-specific synonyms."""
        expander = BookQueryExpander()

        # Book domain terms should have extra synonyms
        synonyms = expander.expand_word("novel", "en")
        assert "fiction" in synonyms or "story" in synonyms

        synonyms = expander.expand_word("author", "en")
        assert "writer" in synonyms or "novelist" in synonyms

    def test_book_synonyms_pt(self):
        """Test Portuguese book-specific synonyms."""
        expander = BookQueryExpander()

        # Portuguese terms should work
        synonyms = expander.expand_word("livro", "pt")
        assert "obra" in synonyms or "escrito" in synonyms

        synonyms = expander.expand_word("autor", "pt")
        assert "escritor" in synonyms or "poeta" in synonyms

    def test_portuguese_stopwords(self):
        """Test Portuguese stopwords filtering."""
        expander = BookQueryExpander()

        # Portuguese stopwords should be filtered
        assert expander.expand_word("o", "pt") == set()
        assert expander.expand_word("a", "pt") == set()
        assert expander.expand_word("os", "pt") == set()

    def test_fallback_to_english(self):
        """Test fallback to English for unsupported languages."""
        expander = BookQueryExpander()

        # For languages without specific support, should fallback to English
        # This allows some expansion even if not perfect
        synonyms = expander.expand_word("book", "es")  # Spanish
        assert len(synonyms) > 0  # Should get English synonyms as fallback

    def test_get_expanded_terms_only(self):
        """Test getting only expansion terms."""
        expander = BookQueryExpander()

        query = "novel author"
        expansions = expander.get_expanded_terms_only(query, "en")

        # Should have expansions for content words
        assert "novel" in expansions or "author" in expansions

    def test_supported_languages(self):
        """Test getting supported languages."""
        expander = BookQueryExpander()
        languages = expander.get_supported_languages()

        assert "en" in languages
        assert "pt" in languages


class TestMultiLanguageStopwords:
    """Test multi-language stopwords functionality."""

    def test_multiple_languages_loaded(self):
        """Test that stopwords from multiple languages are accessible."""
        from src.indexer.stopwords import load_stopwords, get_stopwords_for_language

        # Test English
        en_stopwords = get_stopwords_for_language("en")
        assert "the" in en_stopwords
        assert "and" in en_stopwords

        # Test Portuguese
        pt_stopwords = get_stopwords_for_language("pt")
        assert "o" in pt_stopwords or "a" in pt_stopwords

        # Test Spanish
        es_stopwords = get_stopwords_for_language("es")
        assert len(es_stopwords) > 0

    def test_load_multiple_languages(self):
        """Test loading stopwords for multiple languages at once."""
        from src.indexer.stopwords import load_stopwords

        # Load multiple languages
        stopwords = load_stopwords(["en", "pt", "es"])

        # Should contain stopwords from all languages
        assert "the" in stopwords  # English
        assert "o" in stopwords or "a" in stopwords  # Portuguese

    def test_is_stopword(self):
        """Test stopword detection."""
        from src.indexer.stopwords import is_stopword

        # English
        assert is_stopword("the", "en") is True
        assert is_stopword("book", "en") is False

        # Portuguese
        assert is_stopword("o", "pt") is True
        assert is_stopword("livro", "pt") is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
