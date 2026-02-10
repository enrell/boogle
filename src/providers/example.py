"""
Example Provider Implementation

This is a template/example showing how to implement a new book provider.
Copy this file and customize it for your book source.
"""

from typing import Dict, Iterator, List, Optional, Tuple
from pathlib import Path

from src.providers.base import BaseBookProvider
from src.providers.registry import register_provider


@register_provider
class ExampleProvider(BaseBookProvider):
    """
    Example book provider implementation.

    This is a template showing the minimum required implementation.
    Copy and customize for your own book source.

    To create your own provider:
    1. Copy this file to src/providers/myprovider.py
    2. Rename ExampleProvider to MyProvider
    3. Change source_name to "myprovider"
    4. Implement the required methods
    5. Add @register_provider decorator
    6. Restart the application

    Your provider will be automatically discovered and available!
    """

    def __init__(self, base_url: str = "https://example.com"):
        """
        Initialize the provider.

        You can accept configuration parameters here that will be
        passed from the provider configuration.
        """
        self.base_url = base_url
        self.api_key = None  # Load from config if needed

    @property
    def source_name(self) -> str:
        """
        Unique identifier for this provider.

        This will be stored in the database and used for filtering.
        Must be lowercase, no spaces, unique across all providers.

        Returns:
            str: Provider name (e.g., 'myprovider', 'openlibrary', 'archive_org')
        """
        return "example"

    @property
    def enabled_by_default(self) -> bool:
        """
        Whether this provider is enabled by default.

        Return False if your provider requires API keys or
        additional setup before it can be used.

        Returns:
            bool: True to enable by default, False to require explicit enabling
        """
        return False  # Example is disabled by default

    def iter_book_metadata(self, limit: Optional[int] = None) -> Iterator[Dict]:
        """
        Stream metadata for all available books.

        This is the primary method for bulk ingestion. It should yield
        dictionaries containing book metadata in a standardized format.

        Required fields:
            - source: provider name (should match source_name)
            - book_id: unique identifier within this provider
            - title: book title (optional but highly recommended)
            - author: book author (optional)
            - url: canonical URL for viewing the book

        Optional fields:
            - language: ISO language code (e.g., 'en', 'fr')
            - category: subjects, genres, or categories
            - release_date: publication date
            - illustrator, original_publication, credits
            - copyright_status, downloads
            - cover_url: URL to cover image
            - files: list of {format, url} for downloadable content

        Args:
            limit: Maximum number of books to yield (None for all)

        Yields:
            Dict: Book metadata dictionary
        """
        count = 0

        # Example: iterate over your book source
        # This could be an API, database, file, etc.
        for book in self._fetch_books_from_source():
            if limit and count >= limit:
                break

            yield {
                "source": self.source_name,
                "book_id": str(book["id"]),
                "title": book.get("title"),
                "author": book.get("author"),
                "language": book.get("language", "en"),
                "category": ", ".join(book.get("subjects", [])),
                "url": self.get_book_url(book["id"]),
                "cover_url": book.get("cover_url"),
                "files": [
                    {"format": "pdf", "url": book.get("pdf_url")},
                    {"format": "epub", "url": book.get("epub_url")},
                ]
                if book.get("pdf_url")
                else [],
            }
            count += 1

    def extract_metadata(self, book_id: str) -> Dict[str, object]:
        """
        Fetch fresh metadata for a single book by ID.

        This is used for updating metadata or fetching details
        for a specific book.

        Args:
            book_id: The provider's unique book identifier

        Returns:
            Dict: Book metadata dictionary

        Raises:
            ValueError: If book not found
        """
        # Example: fetch from API
        # response = requests.get(f"{self.base_url}/api/books/{book_id}")
        # book = response.json()

        # Return standardized format
        return {
            "source": self.source_name,
            "book_id": str(book_id),
            "title": f"Book {book_id}",  # Replace with actual fetch
            "author": None,
            "url": self.get_book_url(book_id),
        }

    def download_book(
        self, book_id: str, output_dir: Path, metadata: Optional[Dict] = None
    ) -> Optional[Path]:
        """
        Download book content files.

        Override this if your provider supports downloading full text.
        Try formats in priority order and return the first successful download.

        Args:
            book_id: The book identifier
            output_dir: Directory to save downloaded files
            metadata: Pre-fetched metadata (can avoid re-fetching)

        Returns:
            Path to downloaded file, or None if not supported/failed
        """
        # Example implementation
        import requests

        for fmt, suffix in self.get_format_priorities():
            url = f"{self.base_url}/download/{book_id}{suffix}"
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    path = output_dir / f"{book_id}.{fmt}"
                    path.write_bytes(response.content)
                    return path
            except Exception:
                continue

        return None

    def get_book_url(self, book_id: str) -> str:
        """
        Get canonical URL for viewing the book.

        Args:
            book_id: The book identifier

        Returns:
            str: Full URL to view the book
        """
        return f"{self.base_url}/books/{book_id}"

    def get_cover_url(
        self, book_id: str, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Get cover image URL if available.

        Args:
            book_id: The book identifier
            metadata: Pre-fetched metadata (optional)

        Returns:
            Cover image URL, or None if not available
        """
        return f"{self.base_url}/covers/{book_id}.jpg"

    def get_format_priorities(self) -> List[Tuple[str, str]]:
        """
        Return format download priorities.

        Return list of (format_type, format_suffix) in order of preference.
        Return empty list if downloads are not supported.

        Returns:
            List of (format, suffix) tuples
        """
        return [
            ("txt", ".txt"),
            ("epub", ".epub"),
            ("pdf", ".pdf"),
        ]

    def filter_book(self, metadata: Dict) -> bool:
        """
        Filter out books that should not be indexed.

        Return False to skip indexing this book. Use this to filter out
        unwanted content like dictionaries, test files, etc.

        Args:
            metadata: Book metadata dictionary

        Returns:
            bool: True to index, False to skip
        """
        # Example: skip books without titles
        if not metadata.get("title"):
            return False

        # Example: skip test books
        title = metadata.get("title", "").lower()
        if "test" in title or "sample" in title:
            return False

        return True

    def get_seeder_config(self) -> Dict:
        """
        Get seeder configuration specific to this provider.

        Returns:
            Dict with seeder options
        """
        return {
            "batch_size": 100,  # Process 100 books at a time
            "checkpoint_name": f".checkpoint_{self.source_name}",
        }

    # Helper methods (not part of interface)

    def _fetch_books_from_source(self):
        """
        Helper method to fetch books from your source.

        This is not part of the BaseBookProvider interface.
        Implement your source-specific fetching logic here.
        """
        # This is just a placeholder - replace with actual implementation
        return []

    def _api_request(self, endpoint: str) -> Dict:
        """
        Helper method for API requests.

        Implement authentication, rate limiting, etc. here.
        """
        import requests

        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        response = requests.get(
            f"{self.base_url}/api/{endpoint}", headers=headers, timeout=30
        )
        response.raise_for_status()
        return response.json()


# Example usage (for documentation):
"""
# Creating a new provider:

1. Copy this file:
   cp src/providers/example.py src/providers/myprovider.py

2. Edit myprovider.py:
   - Change class name: ExampleProvider -> MyProvider
   - Change source_name: return "myprovider"
   - Implement iter_book_metadata() to yield books from your source
   - Implement extract_metadata() for single book lookup
   - Optional: Implement download_book() if you support downloads

3. Test your provider:
   python -c "
   from src.providers.registry import ProviderRegistry
   ProviderRegistry.auto_discover()
   provider = ProviderRegistry.get('myprovider')
   for book in provider.iter_book_metadata(limit=5):
       print(book['title'])
   "

4. Enable in configuration (optional):
   Set BOOGLE_PROVIDER_MYPROVIDER_ENABLED=1
   
5. Use in pipeline:
   python src/pipeline.py index --providers myprovider

That's it! Your provider is now fully integrated with Boogle.
"""
