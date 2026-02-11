"""
Query expansion using WordNet synonyms and related terms.

Expands user queries with synonyms to improve recall.
"""

import os
import re
from typing import List, Set, Optional, Dict
from collections import defaultdict


class QueryExpander:
    """
    Expand search queries with synonyms and related terms.

    Uses NLTK WordNet for English synonyms.
    Can be extended with other language WordNets.
    """

    def __init__(self, max_expansions: int = 3, min_word_length: int = 4):
        """
        Initialize query expander.

        Args:
            max_expansions: Maximum number of synonyms per word
            min_word_length: Minimum word length to expand
        """
        self.max_expansions = max_expansions
        self.min_word_length = min_word_length
        self._wordnet = None
        self._stopwords = None

        # Language-specific expanders
        self.lang_expanders: Dict[str, callable] = {
            "en": self._expand_english,
        }

    def _get_wordnet(self):
        """Lazy load WordNet."""
        if self._wordnet is None:
            try:
                from nltk.corpus import wordnet

                # Ensure wordnet is downloaded
                import nltk

                try:
                    nltk.data.find("corpora/wordnet")
                except LookupError:
                    nltk.download("wordnet", quiet=True)

                self._wordnet = wordnet
            except ImportError:
                print("Warning: NLTK not available for query expansion")
                return None
        return self._wordnet

    def _get_stopwords(self) -> Set[str]:
        """Get English stopwords."""
        if self._stopwords is None:
            try:
                from nltk.corpus import stopwords
                import nltk

                try:
                    nltk.data.find("corpora/stopwords")
                except LookupError:
                    nltk.download("stopwords", quiet=True)

                self._stopwords = set(stopwords.words("english"))
            except:
                # Fallback stopwords
                self._stopwords = {
                    "the",
                    "a",
                    "an",
                    "is",
                    "are",
                    "was",
                    "were",
                    "be",
                    "been",
                    "being",
                    "have",
                    "has",
                    "had",
                    "do",
                    "does",
                    "did",
                    "will",
                    "would",
                    "could",
                    "should",
                    "may",
                    "might",
                    "must",
                    "shall",
                    "can",
                    "need",
                    "dare",
                    "ought",
                    "used",
                    "to",
                    "of",
                    "in",
                    "for",
                    "on",
                    "with",
                    "at",
                    "by",
                    "from",
                    "as",
                    "into",
                    "through",
                    "during",
                    "before",
                    "after",
                    "above",
                    "below",
                    "between",
                    "under",
                    "and",
                    "but",
                    "or",
                    "yet",
                    "so",
                    "if",
                    "because",
                    "although",
                    "though",
                    "while",
                    "where",
                    "when",
                    "that",
                    "which",
                    "who",
                    "whom",
                    "whose",
                    "what",
                    "this",
                    "these",
                    "those",
                    "i",
                    "me",
                    "my",
                    "myself",
                    "we",
                    "our",
                    "you",
                    "your",
                    "he",
                    "him",
                    "his",
                    "she",
                    "her",
                    "it",
                    "its",
                    "they",
                    "them",
                    "their",
                    "s",
                    "book",
                    "books",
                }
        return self._stopwords

    def _expand_english(self, word: str) -> Set[str]:
        """
        Get English synonyms from WordNet.

        Args:
            word: Word to expand

        Returns:
            Set of synonyms
        """
        wordnet = self._get_wordnet()
        if not wordnet:
            return set()

        synonyms = set()

        # Get synsets for the word
        synsets = wordnet.synsets(word)

        for synset in synsets[:2]:  # Limit to first 2 synsets
            # Get lemmas (synonyms) from synset
            for lemma in synset.lemmas():
                synonym = lemma.name().replace("_", " ")

                # Skip if same as original or too different
                if synonym.lower() != word.lower():
                    synonyms.add(synonym)

                if len(synonyms) >= self.max_expansions:
                    break

            if len(synonyms) >= self.max_expansions:
                break

        return synonyms

    def expand_word(self, word: str, lang: str = "en") -> Set[str]:
        """
        Expand a single word with synonyms.

        Args:
            word: Word to expand
            lang: Language code

        Returns:
            Set of synonym terms
        """
        # Skip short words and stopwords
        if len(word) < self.min_word_length:
            return set()

        if word.lower() in self._get_stopwords():
            return set()

        # Use language-specific expander
        if lang in self.lang_expanders:
            return self.lang_expanders[lang](word)

        # Fallback to English
        return self._expand_english(word)

    def expand_query(
        self, query: str, lang: str = "en", boost_factor: float = 0.5
    ) -> tuple:
        """
        Expand a full query with synonyms.

        Args:
            query: Original search query
            lang: Language code
            boost_factor: Weight for expanded terms (0.0-1.0)

        Returns:
            Tuple of (expanded_query, expansion_info)
        """
        # Split query into words
        words = re.findall(r"\b\w+\b", query.lower())

        original_terms = []
        expansion_terms = []
        expansion_info = {}

        for word in words:
            original_terms.append(word)

            # Get expansions for this word
            synonyms = self.expand_word(word, lang)

            if synonyms:
                expansion_terms.extend(synonyms)
                expansion_info[word] = list(synonyms)

        # Combine: original query + expanded terms with boost
        if expansion_terms:
            # Deduplicate
            unique_expansions = list(set(expansion_terms))

            # Build expanded query
            expanded = query + " OR " + " OR ".join(unique_expansions)

            # Alternative: just add terms
            # expanded = query + " " + " ".join(unique_expansions)
        else:
            expanded = query

        return expanded, expansion_info

    def get_expanded_terms_only(
        self, query: str, lang: str = "en"
    ) -> Dict[str, List[str]]:
        """
        Get just the expansion terms without building a query.

        Returns:
            Dict mapping original word -> list of expansions
        """
        words = re.findall(r"\b\w+\b", query.lower())
        expansions = {}

        for word in words:
            if len(word) >= self.min_word_length:
                synonyms = self.expand_word(word, lang)
                if synonyms:
                    expansions[word] = list(synonyms)

        return expansions


class BookQueryExpander(QueryExpander):
    """
    Book-specific query expander with domain knowledge.
    """

    # Domain-specific term mappings for books
    BOOK_SYNONYMS = {
        "novel": ["fiction", "story", "book", "tale", "narrative"],
        "author": ["writer", "novelist", "poet", "playwright"],
        "chapter": ["section", "part", "division"],
        "plot": ["storyline", "narrative", "story"],
        "character": ["protagonist", "hero", "figure", "person"],
        "theme": ["motif", "subject", "topic", "idea"],
        "genre": ["category", "type", "kind", "style"],
        "literature": ["writings", "books", "works", "letters"],
    }

    def __init__(self, **kwargs):
        """Initialize with book-specific expansions."""
        super().__init__(**kwargs)

    def expand_word(self, word: str, lang: str = "en") -> Set[str]:
        """Expand with WordNet + book-specific terms."""
        synonyms = super().expand_word(word, lang)

        # Add book-specific synonyms
        if word.lower() in self.BOOK_SYNONYMS:
            synonyms.update(self.BOOK_SYNONYMS[word.lower()])

        # Check reverse mapping
        for term, syns in self.BOOK_SYNONYMS.items():
            if word.lower() in syns:
                synonyms.add(term)

        return synonyms
