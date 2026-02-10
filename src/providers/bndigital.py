"""
Biblioteca Nacional Digital (Brazil) Provider

Provider for the Brazilian National Library's digital collection.
Fetches metadata from BNDigital's Sophia-based catalog system.

Note: This provider focuses on metadata extraction. Full text downloads
may be available through the digital collection interface but require
authentication or specific download links.

Website: https://bndigital.bn.gov.br/acervodigital/
"""

import re
from typing import Dict, Iterator, List, Optional, Tuple
from pathlib import Path
from urllib.parse import urljoin, quote

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


@register_provider
class BNDigitalProvider(BaseBookProvider):
    """
        Brazilian National Library Digital Collection provider.

        Provides access to the digital collection of the Biblioteca Nacional
    digital collection.

        Features:
        - Portuguese and Brazilian literature focus
        - Historical documents and rare books
        - Public domain works

        Example:
            provider = BNDigitalProvider()
            for meta in provider.iter_book_metadata(limit=50):
                print(f"{meta['title']} - {meta['author']}")
    """

    # Base URLs for BNDigital
    BASE_URL = "https://bndigital.bn.gov.br"
    SOPHIA_URL = "https://acervobndigital.bn.gov.br/sophia"

    def __init__(self, base_url: str = None):
        self.base_url = base_url or self.BASE_URL
        self._session = None

    @property
    def session(self):
        """Lazy session initialization."""
        if self._session is None:
            if not REQUESTS_AVAILABLE:
                raise ImportError(
                    "BNDigital provider requires 'requests'. "
                    "Install with: pip install requests"
                )
            if not BS4_AVAILABLE:
                raise ImportError(
                    "BNDigital provider requires 'beautifulsoup4'. "
                    "Install with: pip install beautifulsoup4"
                )
            self._session = requests.Session()
            self._session.headers.update(
                {"User-Agent": "BoogleSearch/1.0 (research@example.com)"}
            )
        return self._session

    @property
    def source_name(self) -> str:
        return "bndigital"

    @property
    def enabled_by_default(self) -> bool:
        """Disabled by default as it requires web scraping."""
        return False

    def _fetch(self, url: str, params: Optional[Dict] = None) -> Optional[str]:
        """Fetch URL content."""
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"BNDigital fetch error: {e}")
            return None

    def _parse_sophia_results(self, html: str) -> List[Dict]:
        """Parse Sophia search results HTML."""
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        results = []

        # Try to find result items in the Sophia interface
        # The structure may vary, so we try multiple selectors
        result_items = (
            soup.find_all("div", class_="resultado")
            or soup.find_all("div", class_="item-resultado")
            or soup.find_all("tr", class_="resultado")
            or soup.select(".resultados tbody tr")
        )

        for item in result_items:
            try:
                # Extract title
                title_elem = (
                    item.find("a", class_="titulo")
                    or item.find("td", class_="titulo")
                    or item.find("h3")
                    or item.find("a")
                )
                title = title_elem.get_text(strip=True) if title_elem else None

                # Extract link
                link = (
                    title_elem.get("href")
                    if title_elem and title_elem.name == "a"
                    else None
                )
                if link:
                    link = urljoin(self.SOPHIA_URL, link)

                # Extract author
                author_elem = (
                    item.find("span", class_="autor")
                    or item.find("td", class_="autor")
                    or item.find(text=re.compile(r"Autor:", re.I))
                )
                author = None
                if author_elem:
                    if hasattr(author_elem, "get_text"):
                        author_text = author_elem.get_text(strip=True)
                    else:
                        author_text = str(author_elem)
                    author = author_text.replace("Autor:", "").strip()

                # Extract ID from link
                book_id = None
                if link:
                    match = re.search(r"[?&]codigo=(\d+)", link)
                    if match:
                        book_id = match.group(1)

                if book_id and title:
                    results.append(
                        {
                            "book_id": book_id,
                            "title": title,
                            "author": author,
                            "url": link
                            or f"{self.SOPHIA_URL}/externo/visualiza.asp?codigo={book_id}",
                        }
                    )

            except Exception as e:
                print(f"Error parsing result item: {e}")
                continue

        return results

    def extract_metadata(self, book_id: str) -> Dict[str, object]:
        """
        Extract metadata from BNDigital record page.

        Args:
            book_id: BNDigital book identifier

        Returns:
            Dict: Book metadata
        """
        # Try to fetch the record page
        url = f"{self.SOPHIA_URL}/externo/visualiza.asp"
        params = {"codigo": book_id}

        html = self._fetch(url, params)

        metadata = {
            "source": self.source_name,
            "book_id": str(book_id),
            "url": f"{self.SOPHIA_URL}/externo/visualiza.asp?codigo={book_id}",
            "title": None,
            "author": None,
            "illustrator": None,
            "release_date": None,
            "language": "pt",  # Most works are in Portuguese
            "category": None,
            "original_publication": None,
            "credits": None,
            "copyright_status": None,
            "downloads": None,
            "files": [],
        }

        if not html:
            return metadata

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Extract title
            title_elem = (
                soup.find("h1")
                or soup.find("h2", class_="titulo")
                or soup.find("td", class_="titulo")
                or soup.find("span", class_="titulo")
            )
            if title_elem:
                metadata["title"] = title_elem.get_text(strip=True)

            # Extract metadata from table rows or definition lists
            # Try to find structured metadata
            for row in soup.find_all(
                ["tr", "div"], class_=re.compile("campo|field", re.I)
            ):
                label_elem = row.find(
                    ["td", "th", "dt", "span"],
                    class_=re.compile("label|etiqueta", re.I),
                )
                value_elem = row.find(
                    ["td", "dd", "span"], class_=re.compile("valor|value", re.I)
                )

                if label_elem and value_elem:
                    label = label_elem.get_text(strip=True).lower()
                    value = value_elem.get_text(strip=True)

                    if "autor" in label or "author" in label:
                        metadata["author"] = value
                    elif "título" in label or "title" in label:
                        metadata["title"] = value
                    elif "data" in label or "date" in label:
                        metadata["release_date"] = value
                        metadata["original_publication"] = value
                    elif "idioma" in label or "language" in label:
                        metadata["language"] = value
                    elif "assunto" in label or "subject" in label:
                        metadata["category"] = value
                    elif "direitos" in label or "copyright" in label:
                        metadata["copyright_status"] = value

            # If still no title, try other methods
            if not metadata["title"]:
                # Look for any heading
                for tag in ["h1", "h2", "h3"]:
                    elem = soup.find(tag)
                    if elem:
                        metadata["title"] = elem.get_text(strip=True)
                        break

        except Exception as e:
            print(f"Error extracting metadata for {book_id}: {e}")

        return metadata

    def iter_book_metadata(self, limit: Optional[int] = None) -> Iterator[Dict]:
        """
        Stream metadata from BNDigital catalog.

        Note: This uses the Sophia search interface to browse the catalog.
        Results may be limited by the search interface.

        Args:
            limit: Maximum number of books to yield

        Yields:
            Dict: Book metadata
        """
        count = 0
        offset = 0

        # Search terms to find books (browse by different terms)
        search_terms = ["a", "e", "i", "o", "u"]  # Vowels to get broad results

        for term in search_terms:
            if limit and count >= limit:
                break

            # Search URL
            search_url = f"{self.SOPHIA_URL}/externo/busca.asp"
            params = {
                "rapida_campo": term,
                "rapida_filtro": "palavra_chave",
            }

            html = self._fetch(search_url, params)
            if not html:
                continue

            results = self._parse_sophia_results(html)

            for result in results:
                if limit and count >= limit:
                    break

                # Fetch full metadata for each result
                full_meta = self.extract_metadata(result["book_id"])

                # Merge with search results
                if result.get("title"):
                    full_meta["title"] = result["title"]
                if result.get("author"):
                    full_meta["author"] = result["author"]

                yield full_meta
                count += 1

            # If we got no results, try next term
            if not results:
                continue

    def get_book_url(self, book_id: str) -> str:
        """Get BNDigital URL for a book."""
        return f"{self.SOPHIA_URL}/externo/visualiza.asp?codigo={book_id}"

    def get_cover_url(
        self, book_id: str, metadata: Optional[Dict] = None
    ) -> Optional[str]:
        """
        Get cover image URL if available.

        BNDigital may have cover images in their viewer.
        """
        # Try to construct a cover URL if available
        # This is provider-specific and may not work for all books
        return None

    def supports_downloads(self) -> bool:
        """BNDigital doesn't provide direct download links easily."""
        return False

    def filter_book(self, metadata: Dict) -> bool:
        """
        Filter out invalid entries.
        """
        # Skip entries without titles
        if not metadata.get("title"):
            return False

        # Skip very short titles (likely not real books)
        title = metadata.get("title", "")
        if len(title) < 3:
            return False

        return True

    def search_books(self, query: str, limit: int = 10) -> List[Dict[str, str]]:
        """
        Search BNDigital catalog.

        Args:
            query: Search query (Portuguese keywords work best)
            limit: Maximum results

        Returns:
            List of book metadata dicts
        """
        search_url = f"{self.SOPHIA_URL}/externo/busca.asp"
        params = {
            "rapida_campo": query,
            "rapida_filtro": "palavra_chave",
        }

        html = self._fetch(search_url, params)
        if not html:
            return []

        results = self._parse_sophia_results(html)

        # Convert to standardized format
        standardized = []
        for result in results[:limit]:
            standardized.append(
                {
                    "source": self.source_name,
                    "book_id": result["book_id"],
                    "title": result.get("title"),
                    "author": result.get("author"),
                    "url": result.get("url") or self.get_book_url(result["book_id"]),
                }
            )

        return standardized
