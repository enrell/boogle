"""
Unified Book Schema

Provides a standardized metadata schema for all book providers.
Includes dataclasses, validation, and translation interfaces.
"""

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
from src.schemas.translator import SchemaTranslator, TranslationContext

__all__ = [
    "UnifiedBookMetadata",
    "FileInfo",
    "Contributor",
    "ImageInfo",
    "RatingInfo",
    "PopularityInfo",
    "ProviderSource",
    "SearchSignals",
    "SchemaTranslator",
    "TranslationContext",
]
