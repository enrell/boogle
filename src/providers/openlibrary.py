"""
Open Library Book Provider

Fetches books from Open Library's public domain collection.
Uses the Open Library API to discover and fetch metadata.

Note: Open Library focuses on metadata - full text downloads may not be
available for all books. This provider is primarily useful for metadata
enrichment and discovering books across multiple editions.
"""

import json
from typing import Dict, Iterator, List, Optional, Tuple
from pathlib import Path
from datetime import datetime

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None

from src.providers.base import BaseBookProvider
from src.providers.registry import register_provider


@register_provider
class OpenLibraryProvider(BaseBookProvider):
    """
    Open Library book provider.

    Open Library is an open, editable library catalog with metadata for
    millions of books. This provider fetches public domain works.

    Note: Open Library provides metadata, not full text downloads.
    Use this provider for metadata enrichment and discovery.

    Example:
        provider = OpenLibraryProvider()
        for meta in provider.iter_book_metadata(limit=100):
            print(f"{meta['title']} by {meta['author']}")
    """

    def __init__(self, base_url: str = "https://openlibrary.org"):
        self.base_url = base_url
        self._session = None

    @property
    def session(self):
        """Lazy session initialization."""
        if self._session is None:
            if not REQUESTS_AVAILABLE:
                raise ImportError(
                    "Open Library provider requires 'requests'. "
                    "Install with: pip install requests"
                )
            self._session = requests.Session()
            self._session.headers.update(
                {"User-Agent": "BoogleSearch/1.0 (boogle@example.com)"}
            )
        return self._session

    @property
    def source_name(self) -> str:
        return "openlibrary"

    @property
    def enabled_by_default(self) -> bool:
        """Disabled by default as it requires API calls."""
        return False

    def _api_request(
        self, endpoint: str, params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """Make API request to Open Library."""
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Open Library API error: {e}")
            return None

    def _format_authors(self, authors: List[Dict]) -> Optional[str]:
        """Format author list into a readable string."""
        if not authors:
            return None

        author_names = []
        for author in authors:
            if isinstance(author, dict):
                name = author.get("name")
                if name:
                    author_names.append(name)

        if not author_names:
            return None

        if len(author_names) == 1:
            return author_names[0]
        elif len(author_names) == 2:
            return f"{author_names[0]} and {author_names[1]}"
        else:
            return f"{author_names[0]} et al."

    def _extract_year(self, date_str: Optional[str]) -> Optional[str]:
        """Extract year from date string."""
        if not date_str:
            return None
        try:
            # Try various date formats
            for fmt in ["%Y-%m-%d", "%Y-%m", "%Y"]:
                try:
                    dt = datetime.strptime(date_str[: len(fmt)], fmt)
                    return str(dt.year)
                except ValueError:
                    continue
            # Try to extract 4-digit year
            import re

            match = re.search(r"\b(19|20)\d{2}\b", date_str)
            if match:
                return match.group(0)
        except Exception:
            pass
        return date_str

    def extract_metadata(self, book_id: str) -> Dict[str, object]:
        """
        Fetch work metadata from Open Library API.

        Args:
            book_id: Open Library work key (e.g., 'OL12345W')

        Returns:
            Dict: Book metadata
        """
        # Remove prefix if present
        if not book_id.startswith("/works/"):
            book_id = f"/works/{book_id}"

        data = self._api_request(f"{book_id}.json")

        if not data:
            return {
                "source": self.source_name,
                "book_id": book_id.replace("/works/", ""),
                "title": None,
                "author": None,
                "url": self.get_book_url(book_id),
            }

        # Extract authors
        authors_data = data.get("authors", [])
        author_names = []
        for author_ref in authors_data:
            if isinstance(author_ref, dict):
                author_key = author_ref.get("author", {}).get("key")
                if author_key:
                    # Fetch author name
                    author_data = self._api_request(f"{author_key}.json")
                    if author_data:
                        name = author_data.get("name") or author_data.get(
                            "personal_name"
                        )
                        if name:
                            author_names.append(name)

        author_str = ", ".join(author_names) if author_names else None

        # Extract subjects
        subjects = data.get("subjects", [])
        category = ", ".join(subjects[:5]) if subjects else None

        # Extract description
        description = data.get("description")
        if isinstance(description, dict):
            description = description.get("value")

        # Extract first publish date
        first_publish_date = data.get("first_publish_date")
        release_date = self._extract_year(first_publish_date)

        return {
            "source": self.source_name,
            "book_id": data.get("key", "").replace("/works/", ""),
            "title": data.get("title"),
            "author": author_str,
            "language": None,  # Works don't have language, editions do
            "category": category,
            "original_publication": release_date,
            "credits": None,
            "copyright_status": "Public Domain"
            if self._is_public_domain(release_date)
            else None,
            "downloads": None,
            "url": self.get_book_url(data.get("key", "")),
            "cover_url": self.get_cover_url(data.get("key", "")),
            "files": [],  # Open Library doesn't provide direct text downloads
        }

    def iter_book_metadata(self, limit: Optional[int] = None) -> Iterator[Dict]:
        """
        Stream public domain works from Open Library.

        Uses the search API to find public domain books.

        Args:
            limit: Maximum number of books to yield

        Yields:
            Dict: Book metadata
        """
        # Search for public domain books
        # We'll search by publication date to find older works
        params = {
            "q": "public_domain:true",
            "sort": "edition_count desc",
            "limit": min(limit or 100, 100),  # API limit is 100
        }

        count = 0
        offset = 0

        while True:
            if limit and count >= limit:
                break

            params["offset"] = offset
            data = self._api_request("/search.json", params)

            if not data or "docs" not in data:
                break

            docs = data.get("docs", [])
            if not docs:
                break

            for doc in docs:
                if limit and count >= limit:
                    break

                # Extract metadata from search result
                work_key = doc.get("key", "").replace("/works/", "")
                if not work_key:
                    continue

                # Get authors
                authors = doc.get("author_name", [])
                author_str = ", ".join(authors[:3]) if authors else None
                if len(authors) > 3:
                    author_str = f"{', '.join(authors[:3])} et al."

                # Get subjects
                subjects = doc.get("subject", [])
                category = ", ".join(subjects[:5]) if subjects else None

                # Get publish year
                publish_year = doc.get("first_publish_year")

                meta = {
                    "source": self.source_name,
                    "book_id": work_key,
                    "title": doc.get("title"),
                    "author": author_str,
                    "language": doc.get("language", [None])[0]
                    if doc.get("language")
                    else None,
                    "category": category,
                    "original_publication": str(publish_year) if publish_year else None,
                    "credits": None,
                    "copyright_status": "Public Domain"
                    if publish_year and publish_year < 1929
                    else None,
                    "downloads": str(doc.get("edition_count", 0)),
                    "url": self.get_book_url(work_key),
                    "cover_url": self.get_cover_url(work_key),
                    "files": [],  # No direct downloads
                }

                yield meta
                count += 1

            offset += len(docs)

            # Safety check
            if len(docs) < params["limit"]:
                break

    def _is_public_domain(self, year: Optional[str]) -> bool:
        """Check if a book is likely public domain based on year."""
        if not year:
            return False
        try:
            year_int = int(year)
            # In US: published before 1929
            return year_int < 1929
        except ValueError:
            return False

    def get_book_url(self, book_id: str) -> str:
        """Get Open Library URL for a work."""
        # Remove prefix if present
        work_id = book_id.replace("/works/", "")
        return f"https://openlibrary.org/works/{work_id}"

    def get_cover_url(
        self, book_id: str, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """Get cover image URL from Open Library Covers API."""
        work_id = book_id.replace("/works/", "")
        # Open Library covers are in the format:
        # https://covers.openlibrary.org/b/id/{cover_id}-M.jpg
        # But we don't have cover_id from work key, so we return None
        # The cover might be available via the work's editions
        return None

    def supports_downloads(self) -> bool:
        """Open Library doesn't provide direct text downloads."""
        return False

    def filter_book(self, metadata: Dict) -> bool:
        """
        Filter out non-book works.
        """
        # Skip works without titles
        if not metadata.get("title"):
            return False

        # Skip works that are likely not books
        title = metadata.get("title", "").lower()
        skip_keywords = ["magazine", "journal", "periodical", "newspaper"]
        if any(kw in title for kw in skip_keywords):
            return False

        return True

    def search_books(self, query: str, limit: int = 10) -> List[Dict[str, str]]:
        """
        Search Open Library for books.

        Args:
            query: Search query
            limit: Maximum results

        Returns:
            List of book metadata dicts
        """
        params = {
            "q": query,
            "limit": limit,
        }

        data = self._api_request("/search.json", params)

        if not data or "docs" not in data:
            return []

        results = []
        for doc in data.get("docs", [])[:limit]:
            work_key = doc.get("key", "").replace("/works/", "")
            if not work_key:
                continue

            authors = doc.get("author_name", [])
            author_str = ", ".join(authors[:2]) if authors else None

            results.append(
                {
                    "source": self.source_name,
                    "book_id": work_key,
                    "title": doc.get("title"),
                    "author": author_str,
                    "url": self.get_book_url(work_key),
                }
            )

        return results
