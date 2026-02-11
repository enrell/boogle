"""
Cross-Reference Service

Merges book metadata from multiple providers into unified records.
"""

from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
import re
from collections import defaultdict

from src.schemas.unified_metadata import (
    UnifiedBookMetadata,
    ProviderSource,
    Contributor,
    FileInfo,
)
from src.schemas.translator_registry import TranslatorRegistry


@dataclass
class CrossReferenceConfig:
    """Configuration for cross-referencing."""

    # Matching thresholds
    title_similarity_threshold: float = 0.85
    author_match_required: bool = True

    # Merge strategy
    prefer_source_order: List[str] = field(
        default_factory=lambda: ["gutenberg", "openlibrary", "pportal"]
    )

    # Field priority (which source to use for each field)
    field_priority: Dict[str, List[str]] = field(
        default_factory=lambda: {
            "description": ["openlibrary", "gutenberg", "pportal"],
            "cover_image": ["openlibrary", "gutenberg"],
            "rating": ["openlibrary"],
        }
    )


class CrossReferenceService:
    """
    Service for merging books from multiple providers.

    Handles:
    - Duplicate detection (same book across providers)
    - Metadata merging (combine fields from multiple sources)
    - Source quality ranking (pick best source for display)
    """

    def __init__(self, config: Optional[CrossReferenceConfig] = None):
        self.config = config or CrossReferenceConfig()
        self.translator_registry = TranslatorRegistry()

    def normalize_title(self, title: str) -> str:
        """Normalize title for comparison."""
        if not title:
            return ""

        # Convert to lowercase
        normalized = title.lower()

        # Remove articles at the start
        normalized = re.sub(r"^(the|a|an|o|a|os|as)\s+", "", normalized)

        # Remove punctuation and extra spaces
        normalized = re.sub(r"[^\w\s]", "", normalized)
        normalized = " ".join(normalized.split())

        return normalized

    def normalize_author(self, author: str) -> str:
        """Normalize author name for comparison."""
        if not author:
            return ""

        # Convert to lowercase
        normalized = author.lower()

        # Remove extra titles
        normalized = re.sub(r"\b(dr|mr|mrs|ms|prof)\.?\s*", "", normalized)

        # Handle "Last, First" vs "First Last"
        if "," in normalized:
            parts = [p.strip() for p in normalized.split(",")]
            normalized = " ".join(reversed(parts))

        # Remove punctuation
        normalized = re.sub(r"[^\w\s]", "", normalized)
        normalized = " ".join(normalized.split())

        return normalized

    def calculate_title_similarity(self, title1: str, title2: str) -> float:
        """Calculate similarity between two titles (0.0-1.0)."""
        norm1 = self.normalize_title(title1)
        norm2 = self.normalize_title(title2)

        if not norm1 or not norm2:
            return 0.0

        # Exact match
        if norm1 == norm2:
            return 1.0

        # One contains the other
        if norm1 in norm2 or norm2 in norm1:
            return 0.95

        # Token-based similarity (Jaccard)
        tokens1 = set(norm1.split())
        tokens2 = set(norm2.split())

        if not tokens1 or not tokens2:
            return 0.0

        intersection = tokens1 & tokens2
        union = tokens1 | tokens2

        return len(intersection) / len(union)

    def authors_match(
        self, authors1: List[Contributor], authors2: List[Contributor]
    ) -> bool:
        """Check if authors match across sources."""
        if not authors1 or not authors2:
            return True  # Can't verify, assume match

        # Get author names
        names1 = {self.normalize_author(a.name) for a in authors1 if a.name}
        names2 = {self.normalize_author(a.name) for a in authors2 if a.name}

        if not names1 or not names2:
            return True

        # Check for any common author
        for name1 in names1:
            for name2 in names2:
                # Exact match
                if name1 == name2:
                    return True
                # One contains the other (e.g., "Mark Twain" vs "Samuel Clemens (Mark Twain)")
                if name1 in name2 or name2 in name1:
                    return True

        return False

    def is_duplicate(
        self, book1: UnifiedBookMetadata, book2: UnifiedBookMetadata
    ) -> bool:
        """Check if two books are duplicates (same work)."""
        # Check canonical_id first
        if book1.canonical_id == book2.canonical_id:
            return True

        # Check title similarity
        title_sim = self.calculate_title_similarity(book1.title, book2.title)
        if title_sim < self.config.title_similarity_threshold:
            return False

        # Check authors
        if self.config.author_match_required:
            if not self.authors_match(book1.authors, book2.authors):
                return False

        return True

    def merge_books(self, books: List[UnifiedBookMetadata]) -> UnifiedBookMetadata:
        """
        Merge multiple book records into one.

        Strategy:
        1. Pick primary source (highest quality score)
        2. Merge metadata from all sources
        3. Keep all sources in all_sources list
        """
        if not books:
            raise ValueError("Cannot merge empty list")

        if len(books) == 1:
            return books[0]

        # Sort by quality score (descending)
        sorted_books = sorted(
            books, key=lambda b: b.primary_source.quality_score, reverse=True
        )

        # Primary book (best source)
        primary = sorted_books[0]

        # Merge all sources (including primary_source from each book)
        all_sources = []
        for book in sorted_books:
            # Add primary source
            all_sources.append(book.primary_source)
            # Add any additional sources
            all_sources.extend(book.all_sources)

        # Remove duplicate sources (same provider)
        seen_providers = set()
        unique_sources = []
        for source in all_sources:
            if source.provider not in seen_providers:
                seen_providers.add(source.provider)
                unique_sources.append(source)

        # Merge fields based on priority
        merged = UnifiedBookMetadata(
            canonical_id=primary.canonical_id,
            title=primary.title,
            primary_source=primary.primary_source,
            all_sources=unique_sources,
            source_count=len(unique_sources),
        )

        # Merge each field
        merged.authors = self._merge_authors([b.authors for b in sorted_books])
        merged.language = primary.language
        merged.languages = self._merge_lists([b.languages for b in sorted_books])
        merged.subjects = self._merge_lists([b.subjects for b in sorted_books])
        merged.categories = self._merge_lists([b.categories for b in sorted_books])

        # Merge subtitle - use first non-empty subtitle
        for book in sorted_books:
            if book.subtitle:
                merged.subtitle = book.subtitle
                break

        # Use best available description
        for source_name in self.config.field_priority.get("description", []):
            for book in sorted_books:
                if book.primary_source.provider == source_name and book.description:
                    merged.description = book.description
                    break
            if merged.description:
                break

        # Use best available cover
        for source_name in self.config.field_priority.get("cover_image", []):
            for book in sorted_books:
                if book.primary_source.provider == source_name and book.cover_image:
                    merged.cover_image = book.cover_image
                    merged.thumbnail_url = book.thumbnail_url
                    break
            if merged.cover_image:
                break

        # Merge ratings
        ratings = [b.rating for b in sorted_books if b.rating]
        if ratings:
            # Use rating with most reviews
            merged.rating = max(ratings, key=lambda r: r.count or 0)

        # Merge popularity
        merged.popularity = self._merge_popularity(
            [b.popularity for b in sorted_books if b.popularity]
        )

        # Files are now in primary_source, no need to merge at top level
        # They remain in each ProviderSource in all_sources

        # Update provider_ids
        merged.provider_ids = {}
        for book in sorted_books:
            merged.provider_ids.update(book.provider_ids)

        # Recalculate completeness
        merged.metadata_completeness = merged.calculate_completeness()

        return merged

    def _merge_authors(
        self, authors_list: List[List[Contributor]]
    ) -> List[Contributor]:
        """Merge author lists, removing duplicates."""
        seen = set()
        merged = []

        for authors in authors_list:
            for author in authors:
                key = self.normalize_author(author.name)
                if key and key not in seen:
                    seen.add(key)
                    merged.append(author)

        return merged

    def _merge_lists(self, lists: List[List[str]]) -> List[str]:
        """Merge lists, removing duplicates."""
        seen = set()
        merged = []

        for lst in lists:
            for item in lst:
                key = item.lower().strip()
                if key and key not in seen:
                    seen.add(key)
                    merged.append(item)

        return merged

    def _merge_popularity(self, popularities: List) -> Optional:
        """Merge popularity info from multiple sources."""
        if not popularities:
            return None

        total_downloads = sum(p.downloads or 0 for p in popularities)
        total_want = sum(p.want_to_read or 0 for p in popularities)
        total_reading = sum(p.currently_reading or 0 for p in popularities)

        from src.schemas.unified_metadata import PopularityInfo

        return PopularityInfo(
            downloads=total_downloads if total_downloads > 0 else None,
            want_to_read=total_want if total_want > 0 else None,
            currently_reading=total_reading if total_reading > 0 else None,
        )

    def cross_reference(
        self, books: List[UnifiedBookMetadata]
    ) -> List[UnifiedBookMetadata]:
        """
        Cross-reference books and merge duplicates.

        Returns a list of unified books with merged metadata from all sources.
        """
        if not books:
            return []

        # Group by canonical_id
        by_canonical = defaultdict(list)
        for book in books:
            by_canonical[book.canonical_id].append(book)

        # Merge each group
        merged = []
        for canonical_id, group in by_canonical.items():
            if len(group) == 1:
                merged.append(group[0])
            else:
                merged_book = self.merge_books(group)
                merged.append(merged_book)

        return merged

    def find_related(
        self, book: UnifiedBookMetadata, candidates: List[UnifiedBookMetadata]
    ) -> List[Tuple[UnifiedBookMetadata, float]]:
        """
        Find related books based on similarity.

        Returns list of (book, similarity_score) tuples.
        """
        related = []

        for candidate in candidates:
            if candidate.canonical_id == book.canonical_id:
                continue

            score = 0.0

            # Title similarity
            title_sim = self.calculate_title_similarity(book.title, candidate.title)
            score += title_sim * 0.4

            # Author overlap
            if self.authors_match(book.authors, candidate.authors):
                score += 0.3

            # Subject overlap
            book_subjects = set(s.lower() for s in book.subjects)
            cand_subjects = set(s.lower() for s in candidate.subjects)
            if book_subjects and cand_subjects:
                overlap = len(book_subjects & cand_subjects)
                score += min(0.3, overlap * 0.1)

            if score > 0.3:  # Lower threshold to catch more related books
                related.append((candidate, score))

        return sorted(related, key=lambda x: x[1], reverse=True)
