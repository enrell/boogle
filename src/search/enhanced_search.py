"""
Enhanced search with multilingual support, spell correction, and query expansion.

Integrates BM25 search with NLP enhancements for better results.
"""

import os
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path

from .language import LanguageDetector
from .spellcheck import SpellCorrector, MultilingualSpellCorrector
from .expansion import BookQueryExpander


class EnhancedSearchResult:
    """Search result with enhancement metadata."""

    def __init__(
        self,
        book_id: str,
        score: float,
        chunk_id: int,
        title: str = "Unknown",
        author: str = "Unknown",
        **kwargs,
    ):
        self.book_id = book_id
        self.score = score
        self.chunk_id = chunk_id
        self.title = title
        self.author = author
        self.metadata = kwargs


class EnhancedSearcher:
    """
    Enhanced search with spell correction, query expansion, and multilingual support.

    Pipeline:
    1. Language detection
    2. Spell correction
    3. Query expansion (synonyms)
    4. BM25 search
    5. Result fusion and ranking
    """

    def __init__(
        self,
        index_dir: Optional[str] = None,
        enable_spellcheck: bool = True,
        enable_expansion: bool = True,
        enable_language_detection: bool = True,
        expansion_boost: float = 0.3,
    ):
        """
        Initialize enhanced searcher.

        Args:
            index_dir: Path to BM25 index
            enable_spellcheck: Enable spell correction
            enable_expansion: Enable query expansion
            enable_language_detection: Enable language detection
            expansion_boost: Weight for expanded terms (0.0-1.0)
        """
        self.index_dir = index_dir or os.getenv("INDEX_DIR", "data/index")
        self.enable_spellcheck = enable_spellcheck
        self.enable_expansion = enable_expansion
        self.enable_language_detection = enable_language_detection
        self.expansion_boost = expansion_boost

        # Initialize components
        self._init_components()

        # Load BM25 searcher
        self._init_searcher()

    def _init_components(self):
        """Initialize NLP components."""
        # Language detector
        if self.enable_language_detection:
            try:
                self.language_detector = LanguageDetector()
            except Exception as e:
                print(f"Warning: Language detection not available: {e}")
                self.language_detector = None
        else:
            self.language_detector = None

        # Spell corrector
        if self.enable_spellcheck:
            try:
                self.spell_corrector = SpellCorrector()
            except Exception as e:
                print(f"Warning: Spell correction not available: {e}")
                self.spell_corrector = None
        else:
            self.spell_corrector = None

        # Query expander
        if self.enable_expansion:
            try:
                self.query_expander = BookQueryExpander(max_expansions=3)
            except Exception as e:
                print(f"Warning: Query expansion not available: {e}")
                self.query_expander = None
        else:
            self.query_expander = None

    def _init_searcher(self):
        """Initialize BM25 searcher."""
        try:
            from rust_bm25 import FileSearcher
            from src.indexer.stopwords import load_stopwords

            self.searcher = FileSearcher(self.index_dir)
            self.stopwords = list(load_stopwords())
            self.searcher.set_stopwords(self.stopwords)
        except Exception as e:
            print(f"Warning: BM25 searcher not available: {e}")
            self.searcher = None

    def process_query(
        self,
        query: str,
        apply_spellcheck: bool = True,
        apply_expansion: bool = True,
        language: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Process query through enhancement pipeline.

        Args:
            query: Original query
            apply_spellcheck: Apply spell correction
            apply_expansion: Apply query expansion
            language: Language code (auto-detected if None)

        Returns:
            Dict with processed query and metadata
        """
        processed = {
            "original_query": query,
            "corrected_query": query,
            "expanded_query": query,
            "language": language,
            "spell_corrections": {},
            "expansions": {},
            "was_corrected": False,
            "was_expanded": False,
        }

        # Step 1: Language detection
        if self.enable_language_detection and self.language_detector and not language:
            lang = self.language_detector.detect(query)
            processed["language"] = lang

        lang = processed["language"] or "en"

        # Step 2: Spell correction
        if apply_spellcheck and self.spell_corrector:
            corrected, was_corrected, corrections = self.spell_corrector.correct_query(
                query
            )
            processed["corrected_query"] = corrected
            processed["spell_corrections"] = corrections
            processed["was_corrected"] = was_corrected

            if was_corrected:
                query = corrected

        # Step 3: Query expansion
        if apply_expansion and self.query_expander:
            expanded, expansions = self.query_expander.expand_query(
                query, lang, self.expansion_boost
            )
            processed["expanded_query"] = expanded
            processed["expansions"] = expansions
            processed["was_expanded"] = len(expansions) > 0

        return processed

    def search(
        self,
        query: str,
        top_k: int = 10,
        apply_spellcheck: bool = True,
        apply_expansion: bool = True,
        use_expanded: bool = True,
        language: Optional[str] = None,
    ) -> Tuple[List[EnhancedSearchResult], Dict[str, Any]]:
        """
        Search with enhancements.

        Args:
            query: Search query
            top_k: Number of results
            apply_spellcheck: Apply spell correction
            apply_expansion: Apply query expansion
            use_expanded: Search with expanded query
            language: Language code

        Returns:
            Tuple of (results, query_info)
        """
        if not self.searcher:
            raise RuntimeError("BM25 searcher not initialized")

        # Process query
        query_info = self.process_query(
            query,
            apply_spellcheck=apply_spellcheck,
            apply_expansion=apply_expansion,
            language=language,
        )

        # Determine search query
        if use_expanded and query_info.get("was_expanded"):
            # Use expanded query for better recall
            search_query = query_info["expanded_query"]
        else:
            search_query = query_info["corrected_query"]

        # Perform BM25 search
        raw_results = self.searcher.search(search_query, top_k * 2)

        # Convert to enhanced results
        results = []
        seen_books = set()

        for book_id, score, chunk_id in raw_results:
            if book_id in seen_books:
                continue

            seen_books.add(book_id)

            # Get metadata (would need DB lookup in real implementation)
            result = EnhancedSearchResult(
                book_id=str(book_id),
                score=score,
                chunk_id=chunk_id,
            )
            results.append(result)

            if len(results) >= top_k:
                break

        return results, query_info

    def search_with_explanation(self, query: str, top_k: int = 10) -> Dict[str, Any]:
        """
        Search with detailed explanation of enhancements.

        Returns:
            Dict with results and full processing info
        """
        results, query_info = self.search(query, top_k)

        # Build explanation
        explanation = {
            "original_query": query_info["original_query"],
            "processed_query": query_info["corrected_query"],
            "search_query": query_info.get(
                "expanded_query", query_info["corrected_query"]
            ),
            "language": query_info.get("language"),
            "enhancements": {},
            "results": [
                {
                    "book_id": r.book_id,
                    "score": r.score,
                    "title": r.title,
                    "author": r.author,
                }
                for r in results
            ],
        }

        # Add spell correction info
        if query_info["was_corrected"]:
            explanation["enhancements"]["spell_correction"] = {
                "applied": True,
                "corrections": query_info["spell_corrections"],
                "corrected_query": query_info["corrected_query"],
            }

        # Add expansion info
        if query_info["was_expanded"]:
            explanation["enhancements"]["query_expansion"] = {
                "applied": True,
                "expansions": query_info["expansions"],
                "expanded_query": query_info["expanded_query"],
            }

        # Add language info
        if query_info.get("language"):
            from .language import LANGUAGE_NAMES

            lang_name = LANGUAGE_NAMES.get(
                query_info["language"], query_info["language"]
            )
            explanation["enhancements"]["language_detection"] = {
                "detected": query_info["language"],
                "name": lang_name,
            }

        return explanation


class CachedEnhancedSearcher(EnhancedSearcher):
    """
    Enhanced searcher with query caching for performance.
    """

    def __init__(self, cache_size: int = 1000, cache_ttl: int = 300, **kwargs):
        """
        Initialize with caching.

        Args:
            cache_size: Maximum number of cached queries
            cache_ttl: Cache time-to-live in seconds
        """
        super().__init__(**kwargs)
        self.cache_size = cache_size
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, Tuple[Any, float]] = {}
        self._cache_hits = 0
        self._cache_misses = 0

    def _get_cache_key(self, query: str, **kwargs) -> str:
        """Generate cache key from query and parameters."""
        import hashlib

        key_data = f"{query}:{kwargs}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def search(
        self,
        query: str,
        top_k: int = 10,
        apply_spellcheck: bool = True,
        apply_expansion: bool = True,
        use_expanded: bool = True,
        language: Optional[str] = None,
    ) -> Tuple[List[EnhancedSearchResult], Dict[str, Any]]:
        """Search with caching."""
        import time

        cache_key = self._get_cache_key(
            query, top_k, apply_spellcheck, apply_expansion, use_expanded, language
        )

        # Check cache
        if cache_key in self._cache:
            result, timestamp = self._cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                self._cache_hits += 1
                return result

        # Perform search
        self._cache_misses += 1
        result = super().search(
            query, top_k, apply_spellcheck, apply_expansion, use_expanded, language
        )

        # Cache result
        if len(self._cache) >= self.cache_size:
            # Simple LRU: remove oldest
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]

        self._cache[cache_key] = (result, time.time())

        return result

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        total = self._cache_hits + self._cache_misses
        hit_rate = self._cache_hits / total if total > 0 else 0

        return {
            "hits": self._cache_hits,
            "misses": self._cache_misses,
            "hit_rate": hit_rate,
            "size": len(self._cache),
        }
