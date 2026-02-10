"""
Base Book Provider Interface

All book providers must implement this interface to be compatible with Boogle's
indexing and search pipeline.
"""

from abc import ABC, abstractmethod
from typing import Iterator, Dict, Optional, List, Tuple
from pathlib import Path


class BaseBookProvider(ABC):
    """
    Abstract base class for all book providers.

    Providers must implement the minimal interface for metadata extraction.
    Downloading full text is optional - providers can choose not to support it
    by returning empty lists from get_format_priorities() or not implementing
    download_book().

    Example:
        @register_provider
        class MyProvider(BaseBookProvider):
            @property
            def source_name(self) -> str:
                return "myprovider"

            def iter_book_metadata(self, limit=None):
                for book in my_source:
                    yield {
                        'source': self.source_name,
                        'book_id': book['id'],
                        'title': book['title'],
                        'author': book['author'],
                        'language': book.get('language'),
                        'category': book.get('subjects'),
                        'url': self.get_book_url(book['id']),
                    }
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """
        Return a unique identifier for this provider.

        This will be stored in the database and used for filtering.
        Examples: 'gutenberg', 'openlibrary', 'archive_org'

        Returns:
            str: Unique provider name (lowercase, no spaces)
        """
        pass

    @property
    def enabled_by_default(self) -> bool:
        """
        Whether this provider is enabled in default configuration.

        Override to return False for providers that require additional
        setup or API keys.

        Returns:
            bool: True if enabled by default, False otherwise
        """
        return True

    @abstractmethod
    def iter_book_metadata(self, limit: Optional[int] = None) -> Iterator[Dict]:
        """
        Stream metadata for all available books from this provider.

        This is the primary method for bulk ingestion. It should yield
        dictionaries containing book metadata in a standardized format.

        Required fields in yielded dicts:
            - source: provider name (should match source_name)
            - book_id: unique identifier within this provider
            - title: book title (optional but recommended)
            - author: book author (optional)
            - language: ISO language code (optional)
            - category: subjects/categories (optional)
            - url: canonical URL for viewing the book
            - files: list of available format URLs (optional)

        Optional fields:
            - illustrator, release_date, original_publication
            - credits, copyright_status, downloads
            - cover_url: URL to cover image

        Args:
            limit: Maximum number of books to yield (None for all)

        Yields:
            Dict: Book metadata dictionary

        Example:
            def iter_book_metadata(self, limit=None):
                count = 0
                for book in self._fetch_catalog():
                    if limit and count >= limit:
                        break
                    yield {
                        'source': self.source_name,
                        'book_id': str(book['id']),
                        'title': book['title'],
                        'author': book.get('author'),
                        'url': self.get_book_url(book['id']),
                    }
                    count += 1
        """
        pass

    @abstractmethod
    def extract_metadata(self, book_id: str) -> Dict[str, object]:
        """
        Fetch fresh metadata for a single book by ID.

        This is used for updating metadata of existing books or fetching
        details for a specific book.

        Args:
            book_id: The provider's unique book identifier

        Returns:
            Dict: Book metadata dictionary with same fields as iter_book_metadata

        Raises:
            ValueError: If book_id is invalid or book not found
        """
        pass

    def download_book(
        self, book_id: str, output_dir: Path, metadata: Optional[Dict] = None
    ) -> Optional[Path]:
        """
        Download book content files to local storage.

        Override this method if your provider supports downloading full text.
        The method should try formats in priority order and return the path
        to the successfully downloaded file.

        Args:
            book_id: The book identifier
            output_dir: Directory to save downloaded files
            metadata: Pre-fetched metadata (optional, can avoid re-fetching)

        Returns:
            Optional[Path]: Path to downloaded file, or None if download failed
            or provider doesn't support downloads

        Example:
            def download_book(self, book_id, output_dir, metadata=None):
                meta = metadata or self.extract_metadata(book_id)
                for fmt, suffix in self.get_format_priorities():
                    url = f"{self.base_url}/download/{book_id}{suffix}"
                    try:
                        response = requests.get(url)
                        if response.status_code == 200:
                            path = output_dir / f"{book_id}.{fmt}"
                            path.write_bytes(response.content)
                            return path
                    except Exception:
                        continue
                return None
        """
        return None

    @abstractmethod
    def get_book_url(self, book_id: str) -> str:
        """
        Get the canonical URL for viewing this book on the provider's website.

        Args:
            book_id: The book identifier

        Returns:
            str: Full URL to view the book
        """
        pass

    def get_cover_url(
        self, book_id: str, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Get the URL for the book's cover image, if available.

        Args:
            book_id: The book identifier
            metadata: Pre-fetched metadata (optional)

        Returns:
            Optional[str]: Cover image URL, or None if not available
        """
        return None

    def get_format_priorities(self) -> List[Tuple[str, str]]:
        """
        Return list of (format_type, format_suffix) tuples in priority order.

        This defines which formats to try when downloading, in order of preference.
        Override if your provider supports downloads.

        Returns:
            List[Tuple[str, str]]: List of (format, suffix) pairs

        Example:
            return [
                ("txt", ".txt"),      # Prefer plain text
                ("epub", ".epub"),   # Then EPUB
                ("pdf", ".pdf"),     # Finally PDF
            ]
        """
        return []

    def supports_downloads(self) -> bool:
        """
        Check if this provider supports downloading full text.

        Default implementation checks if get_format_priorities() returns
        any formats. Override if your logic is different.

        Returns:
            bool: True if downloads are supported, False otherwise
        """
        return bool(self.get_format_priorities())

    def filter_book(self, metadata: Dict) -> bool:
        """
        Filter out books that should not be indexed.

        Override to implement provider-specific filtering logic.
        Return False to skip indexing this book.

        Common use cases:
            - Skip dictionaries, encyclopedias
            - Filter by language
            - Skip books with missing required fields

        Args:
            metadata: Book metadata dictionary

        Returns:
            bool: True to index this book, False to skip

        Example:
            def filter_book(self, metadata):
                # Skip dictionaries
                title = (metadata.get('title') or '').lower()
                return 'dictionary' not in title
        """
        return True

    def get_seeder_config(self) -> Dict:
        """
        Get configuration specific to the BookSeeder for this provider.

        This allows providers to customize seeding behavior.

        Returns:
            Dict: Configuration options

        Example:
            return {
                'batch_size': 500,  # Smaller batches for this provider
                'checkpoint_name': f'.checkpoint_{self.source_name}',
            }
        """
        return {}
