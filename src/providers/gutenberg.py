"""
Project Gutenberg Book Provider

Implements the BaseBookProvider interface for Project Gutenberg.
"""

import csv
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None
try:
    from bs4 import BeautifulSoup

    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    BeautifulSoup = None

from src.providers.base import BaseBookProvider
from src.providers.registry import register_provider


# Thread-local storage for requests session
_local = threading.local()


def _get_session() -> requests.Session:
    """Get or create thread-local requests session."""
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )
    return _local.session


@register_provider
class GutenbergProvider(BaseBookProvider):
    """
    Project Gutenberg book provider.

    Provides access to Project Gutenberg's catalog of free eBooks.
    Supports both metadata extraction and full text downloads.

    Example:
        provider = GutenbergProvider()
        for meta in provider.iter_book_metadata(limit=100):
            print(f"{meta['title']} by {meta['author']}")
    """

    FORMAT_PRIORITY = [
        ("txt", ".txt.utf-8"),
        ("txt", ".txt"),
        ("epub", ".epub.noimages"),
        ("epub", ".epub.images"),
        ("pdf", ".pdf"),
    ]

    STOP_WORDS = {
        "dictionary",
        "encyclopedia",
        "thesaurus",
        "full text",
        "complete works",
        "webster's",
        "unabridged",
    }

    def __init__(self, base_url: str = "https://www.gutenberg.org"):
        self.base_url = base_url

    @property
    def source_name(self) -> str:
        return "gutenberg"

    def _fetch(self, url: str) -> str:
        """Fetch URL content with proper headers."""
        session = _get_session()
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.text

    def get_book_url(self, book_id: str) -> str:
        """Get canonical URL for viewing the book."""
        return f"{self.base_url}/ebooks/{book_id}"

    def get_cover_url(
        self, book_id: str, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """Get cover image URL for a book."""
        return f"{self.base_url}/cache/epub/{book_id}/pg{book_id}.cover.medium.jpg"

    def get_format_priorities(self) -> List[Tuple[str, str]]:
        """Return format download priorities."""
        return self.FORMAT_PRIORITY

    def extract_metadata(self, book_id: str) -> Dict[str, object]:
        """
        Extract metadata from a Gutenberg book page.

        Args:
            book_id: Gutenberg book ID

        Returns:
            Dict: Book metadata
        """
        url = self.get_book_url(book_id)
        html = self._fetch(url)
        soup = BeautifulSoup(html, "html.parser")

        metadata = {
            "source": self.source_name,
            "book_id": str(book_id),
            "url": url,
            "title": None,
            "author": None,
            "illustrator": None,
            "release_date": None,
            "language": None,
            "category": None,
            "original_publication": None,
            "credits": None,
            "copyright_status": None,
            "downloads": None,
            "files": [],
        }

        # Extract title from header
        title_elem = soup.find("h1", {"id": "book_title"})
        if title_elem:
            title = title_elem.get_text(strip=True)
            if " by " in title:
                title = title.split(" by ")[0].strip()
            metadata["title"] = title

        # Extract from bibrec table
        bibrec_table = soup.find("table", class_="bibrec")
        if bibrec_table:
            for row in bibrec_table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if th and td:
                    key = th.get_text(strip=True).lower()
                    value = td.get_text(strip=True)

                    if key == "author":
                        author_link = td.find("a")
                        metadata["author"] = (
                            author_link.get_text(strip=True) if author_link else value
                        )
                    elif key == "illustrator":
                        illustrator_link = td.find("a")
                        metadata["illustrator"] = (
                            illustrator_link.get_text(strip=True)
                            if illustrator_link
                            else value
                        )
                    elif key == "title":
                        metadata["title"] = (
                            value if not metadata["title"] else metadata["title"]
                        )
                    elif "release date" in key:
                        metadata["release_date"] = value
                    elif key == "language":
                        metadata["language"] = value
                    elif key == "category":
                        metadata["category"] = value
                    elif "original publication" in key:
                        metadata["original_publication"] = value
                    elif key == "credits":
                        metadata["credits"] = value
                    elif "copyright status" in key:
                        metadata["copyright_status"] = value
                    elif key == "downloads":
                        metadata["downloads"] = value

        # Extract file links and normalize format names
        format_map = {
            "plain text": "txt",
            "txt": "txt",
            "epub": "epub",
            "pdf": "pdf",
            "html": "html",
            "mobi": "mobi",
            "kindle": "mobi",
        }

        files_table = soup.find("table", class_="files")
        if files_table:
            for row in files_table.find_all("tr"):
                link = row.find("a", class_="link")
                if link:
                    href_value = link.get("href") or ""
                    href = (
                        href_value[0]
                        if isinstance(href_value, list)
                        else str(href_value)
                    )
                    text = link.get_text(strip=True).lower()
                    if href:
                        full_url = (
                            href
                            if isinstance(href, str) and href.startswith("http")
                            else f"{self.base_url}{href}"
                        )
                        # Normalize format name
                        fmt = format_map.get(text, text)
                        metadata["files"].append({"format": fmt, "url": full_url})

        return metadata

    def iter_book_metadata(self, limit: Optional[int] = None) -> Iterator[Dict]:
        """
        Stream metadata from Gutenberg catalog CSV feed.

        Args:
            limit: Maximum number of books to yield

        Yields:
            Dict: Book metadata
        """
        feed_url = f"{self.base_url}/cache/epub/feeds/pg_catalog.csv"

        # Stream the CSV to avoid loading 100MB+ into RAM
        with requests.get(
            feed_url, headers={"User-Agent": "Mozilla/5.0"}, stream=True
        ) as r:
            r.raise_for_status()
            # Decode lines on the fly
            lines = (line.decode("utf-8", errors="replace") for line in r.iter_lines())

            # Skip BOM if present
            try:
                first_line = next(lines)
                if first_line.startswith("\ufeff"):
                    first_line = first_line[1:]
                from itertools import chain

                lines = chain([first_line], lines)
            except StopIteration:
                return

            reader = csv.DictReader(lines)
            count = 0
            for row in reader:
                book_id = row.get("Text#")
                if not book_id:
                    continue

                # Build download URLs using Gutenberg's standard URL patterns
                files = []
                base_download = f"https://www.gutenberg.org"

                # Standard Gutenberg download URLs
                files.append(
                    {
                        "format": "txt",
                        "url": f"{base_download}/ebooks/{book_id}.txt.utf-8",
                    }
                )
                files.append(
                    {
                        "format": "epub",
                        "url": f"{base_download}/ebooks/{book_id}.epub.noimages",
                    }
                )
                files.append(
                    {
                        "format": "epub",
                        "url": f"{base_download}/ebooks/{book_id}.epub.images",
                    }
                )
                files.append(
                    {
                        "format": "pdf",
                        "url": f"{base_download}/files/{book_id}/{book_id}-pdf.pdf",
                    }
                )

                meta = {
                    "source": self.source_name,
                    "book_id": str(book_id),
                    "url": f"https://www.gutenberg.org/ebooks/{book_id}",
                    "title": row.get("Title"),
                    "author": row.get("Authors"),
                    "language": row.get("Language"),
                    "category": row.get("Subjects"),
                    "release_date": row.get("Issued"),
                    "files": files,
                }

                yield meta
                count += 1
                if limit and count >= limit:
                    break

    def download_book(
        self, book_id: str, output_dir: Path, metadata: Optional[Dict] = None
    ) -> Optional[Path]:
        """
        Download book in best available format.

        Args:
            book_id: Gutenberg book ID
            output_dir: Directory to save file
            metadata: Pre-fetched metadata (optional)

        Returns:
            Path to downloaded file, or None if failed
        """
        session = _get_session()
        base_url = f"{self.base_url}/ebooks/{book_id}"

        for fmt_type, suffix in self.FORMAT_PRIORITY:
            ext = ".txt" if fmt_type == "txt" else f".{fmt_type}"
            filepath = output_dir / f"{book_id}{ext}"

            if filepath.exists():
                return filepath

            url = f"{base_url}{suffix}"
            try:
                resp = session.get(url, timeout=30, allow_redirects=True)
                if resp.status_code == 200 and len(resp.content) > 100:
                    filepath.write_bytes(resp.content)
                    return filepath
            except Exception:
                continue

        return None

    def filter_book(self, metadata: Dict) -> bool:
        """
        Filter out dictionaries, encyclopedias, and other super-documents.

        Args:
            metadata: Book metadata

        Returns:
            bool: True to index, False to skip
        """
        title = (metadata.get("title") or "").lower()
        return not any(word in title for word in self.STOP_WORDS)

    def search_books(self, query: str, limit: int = 10) -> List[Dict[str, str]]:
        """
        Search Gutenberg for books matching query.

        Args:
            query: Search query
            limit: Maximum results

        Returns:
            List of book metadata dicts
        """
        search_url = f"{self.base_url}/ebooks/search/?query={query}&submit_search=Go%21"
        html = self._fetch(search_url)
        soup = BeautifulSoup(html, "html.parser")

        results = []
        book_links = soup.find_all("a", href=re.compile(r"/ebooks/(\d+)"))

        seen_ids = set()
        for link in book_links[:limit]:
            href_value = link.get("href") or ""
            href = href_value[0] if isinstance(href_value, list) else str(href_value)
            match = re.search(r"/ebooks/(\d+)", href)
            if match:
                book_id = match.group(1)
                if book_id not in seen_ids:
                    seen_ids.add(book_id)
                    results.append(
                        {
                            "source": self.source_name,
                            "book_id": str(book_id),
                            "title": link.get_text(strip=True),
                            "url": f"{self.base_url}{href}",
                        }
                    )

        return results

    def get_seeder_config(self) -> Dict:
        """Get seeder configuration for Gutenberg."""
        return {
            "batch_size": 500,
            "checkpoint_name": ".checkpoint",
        }
