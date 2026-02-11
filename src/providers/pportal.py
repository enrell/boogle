"""
PPORTAL - Public Domain Portuguese-language Literature Dataset Provider

Provider for the PPORTAL dataset containing Portuguese public domain works
from three Brazilian digital libraries:
- Domínio Público (http://www.dominiopublico.gov.br)
- Projeto Adamastor (http://www.projektoadamastor.org)
- BLPL (Biblioteca de Literatura de Projeto)

The dataset is available from Zenodo and includes 82,313+ works.

Dataset: https://doi.org/10.5281/zenodo.5178063
Paper: https://sol.sbc.org.br/index.php/dsw/article/view/17416
"""

import csv
import re
from pathlib import Path
from typing import Dict, Iterator, List, Optional
import zipfile

try:
    import requests

    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None

from src.providers.base import BaseBookProvider
from src.providers.registry import register_provider


@register_provider
class PPORTALProvider(BaseBookProvider):
    """
    PPORTAL Public Domain Portuguese Literature Dataset Provider.

    Provides access to Portuguese public domain works with metadata
    from multiple Brazilian digital libraries.

    Features:
    - 82,313+ public domain works (from Domínio Público)
    - Portuguese language literature
    - Direct download links to PDFs

    The dataset is downloaded once from Zenodo and cached locally.

    Example:
        provider = PPORTALProvider()
        for meta in provider.iter_book_metadata(limit=100):
            print(f"{meta['title']} by {meta['author']}")
            print(f"Download: {meta['url']}")
    """

    ZENODO_URL = "https://zenodo.org/record/5178063/files/PPORTAL.zip"
    DATASET_CACHE_DIR = "data/pportal"

    def __init__(self, cache_dir: str = None):
        self.cache_dir = Path(cache_dir or self.DATASET_CACHE_DIR)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._works_data = None
        self._session = None

    @property
    def source_name(self) -> str:
        return "pportal"

    @property
    def enabled_by_default(self) -> bool:
        return True

    @property
    def session(self):
        if self._session is None:
            if not REQUESTS_AVAILABLE:
                raise ImportError(
                    "PPORTAL requires 'requests'. Install: pip install requests"
                )
            self._session = requests.Session()
            self._session.headers.update({"User-Agent": "BoogleSearch/1.0"})
        return self._session

    def _download_dataset(self) -> bool:
        """Download and extract dataset from Zenodo."""
        zip_path = self.cache_dir / "PPORTAL.zip"

        if (self.cache_dir / "digital_library_dominio.csv").exists():
            return True

        print(f"Downloading PPORTAL dataset from Zenodo...")
        try:
            response = self.session.get(self.ZENODO_URL, stream=True, timeout=120)
            response.raise_for_status()

            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(self.cache_dir)

            zip_path.unlink()
            print(f"  Dataset ready!")
            return True

        except Exception as e:
            print(f"Error: {e}")
            if zip_path.exists():
                zip_path.unlink()
            return False

    def _load_csv(self, filename: str, delimiter: str = "\t") -> List[Dict]:
        """Load a CSV file."""
        filepath = self.cache_dir / filename
        if not filepath.exists():
            return []

        works = []
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                works.append(dict(row))
        return works

    def _load_works_data(self) -> List[Dict]:
        """Load and merge works data from multiple CSVs."""
        if self._works_data is not None:
            return self._works_data

        if not self._download_dataset():
            return []

        # Load metadata CSV (has titles, authors, format)
        metadata = self._load_csv("digital_library_dominio.csv", delimiter="\t")
        print(f"  Loaded {len(metadata)} works from dominio.csv")

        # Load links CSV (has download links)
        links = self._load_csv("digital_library_preliminary.csv", delimiter="\t")
        print(f"  Loaded {len(links)} works from preliminary.csv")

        # Create lookup for download links by ID
        link_map = {}
        for row in links:
            work_id = row.get("original_id", "")
            download_link = row.get("download_link", "")
            if work_id and download_link:
                link_map[work_id] = download_link

        # Merge: add download links to metadata
        merged = []
        for row in metadata:
            work_id = row.get("original_id", "")
            if work_id in link_map:
                row["download_link"] = link_map[work_id]
            merged.append(row)

        print(f"  Merged {len(merged)} works with metadata and links")
        self._works_data = merged
        return merged

    def _extract_obra_id(self, download_link: str) -> Optional[str]:
        """Extract co_obra ID from Domínio Público URL."""
        if not download_link:
            return None
        match = re.search(r"co_obra=(\d+)", download_link)
        if match:
            return match.group(1)
        return None

    def _parse_work_row(self, row: Dict) -> Optional[Dict]:
        """Parse a work row into standardized metadata."""
        title = row.get("work_title", "")
        author = row.get("work_authors", "")

        if not title:
            return None

        # Get download link
        download_link = row.get("download_link", "")

        # Extract actual book ID from download link (co_obra parameter)
        obra_id = self._extract_obra_id(download_link)
        if obra_id:
            work_id = obra_id
        else:
            # Fallback to original_id
            work_id = row.get("original_id", "")

        # Build files list if download link available
        files = []
        if download_link:
            file_format = row.get("file_format", ".pdf")
            files.append({"format": file_format.replace(".", ""), "url": download_link})

        return {
            "source": self.source_name,
            "book_id": str(work_id),
            "title": title.strip(),
            "author": author.strip() if author else None,
            "language": "pt",
            "category": row.get("original_source", ""),
            "copyright_status": "Public Domain",
            "url": download_link if download_link else self.get_book_url(str(work_id)),
            "files": files,
            "format": row.get("file_format", "").replace(".", "")
            if row.get("file_format")
            else None,
            "file_size": row.get("file_size", ""),
        }

    def iter_book_metadata(self, limit: Optional[int] = None) -> Iterator[Dict]:
        """Stream book metadata from PPORTAL dataset."""
        works = self._load_works_data()

        if not works:
            print("Warning: No PPORTAL data available")
            return

        count = 0
        for row in works:
            if limit and count >= limit:
                break

            meta = self._parse_work_row(row)
            if meta:
                yield meta
                count += 1

    def extract_metadata(self, book_id: str) -> Dict[str, object]:
        """Extract metadata for a single work by ID."""
        works = self._load_works_data()

        for row in works:
            # Check if book_id matches obra_id from download link
            download_link = row.get("download_link", "")
            obra_id = self._extract_obra_id(download_link)

            if obra_id and str(obra_id) == str(book_id):
                meta = self._parse_work_row(row)
                if meta:
                    return meta
            # Also check original_id as fallback
            elif str(row.get("original_id", "")) == str(book_id):
                meta = self._parse_work_row(row)
                if meta:
                    return meta

        return {
            "source": self.source_name,
            "book_id": str(book_id),
            "title": None,
            "author": None,
            "language": "pt",
            "url": self.get_book_url(book_id),
            "files": [],
        }

    def get_book_url(self, book_id: str) -> str:
        """Get URL for the work."""
        return f"http://www.dominiopublico.gov.br/pesquisa/DetalheObraForm.do?select_action=&co_obra={book_id}"

    def supports_downloads(self) -> bool:
        """PPORTAL provides download links."""
        return True

    def download_book(
        self, book_id: str, output_dir: Path, metadata: Optional[Dict] = None
    ) -> Optional[Path]:
        """
        Download book from Domínio Público.

        Note: Domínio Público requires session cookies, so direct download
        may not work. Returns the URL for manual download.
        """
        meta = metadata or self.extract_metadata(book_id)
        files = meta.get("files", [])

        if not files:
            print(f"No download link available for book {book_id}")
            return None

        download_url = files[0]["url"]
        print(f"Download URL for {book_id}: {download_url}")
        print(
            f"Note: Domínio Público requires browser session. Download manually from the URL above."
        )

        # Try to download anyway
        try:
            response = self.session.get(download_url, timeout=30, allow_redirects=True)
            if response.status_code == 200 and len(response.content) > 1000:
                ext = files[0].get("format", "pdf")
                filepath = output_dir / f"{book_id}.{ext}"
                filepath.write_bytes(response.content)
                print(f"  Downloaded to {filepath}")
                return filepath
        except Exception as e:
            print(f"  Download failed: {e}")

        return None

    def filter_book(self, metadata: Dict) -> bool:
        """Filter out invalid entries."""
        if not metadata.get("title"):
            return False

        title = metadata.get("title", "").strip()
        if len(title) < 3:
            return False

        return True

    def search_books(self, query: str, limit: int = 10) -> List[Dict[str, str]]:
        """Search PPORTAL dataset for books."""
        works = self._load_works_data()

        query_lower = query.lower()
        results = []

        for row in works:
            if len(results) >= limit:
                break

            title = (row.get("work_title") or "").lower()
            author = (row.get("work_authors") or "").lower()

            if query_lower in title or query_lower in author:
                work_id = row.get("original_id", "")
                download_link = row.get("download_link", "")
                results.append(
                    {
                        "source": self.source_name,
                        "book_id": str(work_id),
                        "title": row.get("work_title"),
                        "author": row.get("work_authors"),
                        "url": download_link
                        if download_link
                        else self.get_book_url(str(work_id)),
                    }
                )

        return results

    def get_dataset_info(self) -> Dict:
        """Get information about the loaded dataset."""
        works = self._load_works_data()

        # Count how many have download links
        with_links = sum(1 for w in works if w.get("download_link"))

        return {
            "total_works": len(works),
            "works_with_download_links": with_links,
            "cache_dir": str(self.cache_dir),
            "dataset_url": self.ZENODO_URL,
        }
