"""
Spell correction using SymSpell for fuzzy matching.

SymSpell is 1000x+ faster than BK-tree/Levenshtein based approaches.
"""

import os
from typing import List, Optional, Dict
from pathlib import Path
from symspellpy import SymSpell, Verbosity


class SpellCorrector:
    """
    Fast spell correction with SymSpell.

    Features:
    - Prefix indexing for O(1) lookup
    - Configurable edit distance (default: 2)
    - Support for custom dictionaries
    """

    def __init__(
        self,
        max_edit_distance: int = 2,
        prefix_length: int = 7,
        dictionary_path: Optional[str] = None,
    ):
        """
        Initialize spell corrector.

        Args:
            max_edit_distance: Maximum edit distance for suggestions (1-3)
            prefix_length: Prefix length for indexing (higher = faster, less accurate)
            dictionary_path: Path to custom dictionary file
        """
        self.symspell = SymSpell(max_edit_distance, prefix_length)
        self.max_edit_distance = max_edit_distance

        # Load dictionary if provided
        if dictionary_path and os.path.exists(dictionary_path):
            self._load_dictionary(dictionary_path)

    def _load_dictionary(self, path: str):
        """Load frequency dictionary from file."""
        self.symspell.load_dictionary(path, term_index=0, count_index=1)

    def correct_word(
        self, word: str, verbosity: Verbosity = Verbosity.CLOSEST
    ) -> Optional[str]:
        """
        Get best correction for a single word.

        Args:
            word: Word to correct
            verbosity: Verbosity level (CLOSEST, TOP, ALL)

        Returns:
            Corrected word or None if no correction found
        """
        if not word or len(word) < 3:
            return None

        suggestions = self.symspell.lookup(word, verbosity, self.max_edit_distance)

        if suggestions:
            return suggestions[0].term
        return None

    def suggest_words(self, word: str, max_suggestions: int = 5) -> List[str]:
        """
        Get multiple suggestions for a word.

        Args:
            word: Word to correct
            max_suggestions: Maximum number of suggestions

        Returns:
            List of suggested corrections
        """
        if not word or len(word) < 3:
            return []

        suggestions = self.symspell.lookup(word, Verbosity.ALL, self.max_edit_distance)

        return [s.term for s in suggestions[:max_suggestions]]

    def correct_query(self, query: str) -> tuple:
        """
        Correct a search query word by word.

        Args:
            query: Search query string

        Returns:
            Tuple of (corrected_query, was_corrected, corrections)
        """
        words = query.split()
        corrected_words = []
        corrections = {}
        was_corrected = False

        for word in words:
            # Skip short words and numbers
            if len(word) < 4 or word.isdigit():
                corrected_words.append(word)
                continue

            # Check if correction exists
            suggestion = self.correct_word(word)

            if suggestion and suggestion.lower() != word.lower():
                corrected_words.append(suggestion)
                corrections[word] = suggestion
                was_corrected = True
            else:
                corrected_words.append(word)

        corrected_query = " ".join(corrected_words)
        return corrected_query, was_corrected, corrections

    def add_words(self, words: Dict[str, int]):
        """
        Add words to the dictionary with frequencies.

        Args:
            words: Dict mapping word -> frequency
        """
        for word, count in words.items():
            self.symspell.create_dictionary_entry(word, count)

    @classmethod
    def from_corpus(cls, corpus_path: str, **kwargs) -> "SpellCorrector":
        """
        Build spell corrector from a text corpus.

        Args:
            corpus_path: Path to corpus file (one word per line or text)

        Returns:
            Configured SpellCorrector instance
        """
        corrector = cls(**kwargs)

        # Count word frequencies from corpus
        from collections import Counter

        word_counts = Counter()
        with open(corpus_path, "r", encoding="utf-8") as f:
            for line in f:
                words = line.strip().lower().split()
                word_counts.update(words)

        # Add to dictionary
        corrector.add_words(dict(word_counts))

        return corrector


class MultilingualSpellCorrector:
    """
    Language-aware spell correction supporting multiple languages.
    """

    def __init__(self):
        """Initialize empty multilingual corrector."""
        self.correctors: Dict[str, SpellCorrector] = {}

    def add_language(self, lang_code: str, corrector: SpellCorrector):
        """Add a spell corrector for a language."""
        self.correctors[lang_code] = corrector

    def correct(self, query: str, lang_code: Optional[str] = None) -> tuple:
        """
        Correct query using language-specific corrector.

        Args:
            query: Search query
            lang_code: Language code (e.g., 'en', 'pt') or None for generic

        Returns:
            Tuple of (corrected_query, corrections_made)
        """
        if lang_code and lang_code in self.correctors:
            return self.correctors[lang_code].correct_query(query)

        # Try generic corrector or first available
        if self.correctors:
            return next(iter(self.correctors.values())).correct_query(query)

        return query, False, {}
