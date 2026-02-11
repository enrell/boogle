"""
Boogle Search Enhancement Module

Provides multilingual support, spell correction, query expansion,
and hybrid search capabilities.
"""

from .language import LanguageDetector
from .spellcheck import SpellCorrector
from .expansion import QueryExpander
from .enhanced_search import EnhancedSearcher

__all__ = [
    "LanguageDetector",
    "SpellCorrector",
    "QueryExpander",
    "EnhancedSearcher",
]
