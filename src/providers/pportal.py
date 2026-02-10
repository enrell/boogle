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
import os
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
    - Metadata from multiple sources

    The dataset is downloaded once from Zenodo and cached locally.

    Example:
        provider = PPORTALProvider()
        for meta in provider.iter_book_metadata(limit=100):
            print(f"{meta['title']} by {meta['author']}")
    """

    # Zenodo dataset URL
    ZENODO_URL = "https://zenodo.org/record/5178063/files/PPORTAL.zip"
    DATASET_CACHE_DIR = "data/pportal"

    # CSV files in the dataset (in order of preference)
    WORKS_CSV_OPTIONS = [
        "digital_library_dominio.csv",  # Best source with metadata
        "digital_library_preliminary.csv",
    ]

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
        """Enabled by default but requires initial download."""
        return True

    @property
    def session(self):
        """Lazy session initialization."""
        if self._session is None:
            if not REQUESTS_AVAILABLE:
                raise ImportError(
                    "PPORTAL provider requires 'requests'. "
                    "Install with: pip install requests"
                )
            self._session = requests.Session()
            self._session.headers.update(
                {"User-Agent": "BoogleSearch/1.0 (boogle@example.com)"}
            )
        return self._session

    def _download_dataset(self) -> bool:
        """Download and extract the PPORTAL dataset from Zenodo."""
        zip_path = self.cache_dir / "PPORTAL.zip"

        # Check if already extracted
        for csv_option in self.WORKS_CSV_OPTIONS:
            works_file = self.cache_dir / csv_option
            if works_file.exists():
                print(f"PPORTAL dataset already cached at {works_file}")
                return True

        print(f"Downloading PPORTAL dataset from Zenodo...")
        print(f"URL: {self.ZENODO_URL}")
        print(f"This may take a moment (8 MB)...")

        try:
            response = self.session.get(self.ZENODO_URL, stream=True, timeout=120)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0
            chunk_size = 8192

            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0 and downloaded % (1024 * 1024) == 0:
                            print(
                                f"  Downloaded {downloaded / 1024 / 1024:.1f} MB / {total_size / 1024 / 1024:.1f} MB"
                            )

            print(f"  Downloaded to {zip_path}")

            # Extract
            print(f"Extracting...")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(self.cache_dir)

            print(f"  Extracted to {self.cache_dir}")

            # Remove zip file to save space
            zip_path.unlink()
            print(f"  Dataset ready!")

            return True

        except Exception as e:
            print(f"Error downloading PPORTAL dataset: {e}")
            if zip_path.exists():
                zip_path.unlink()
            return False

    def _load_works_data(self) -> List[Dict]:
        """Load works data from CSV."""
        if self._works_data is not None:
            return self._works_data

        # Ensure dataset is downloaded
        if not self._download_dataset():
            return []

        # Find the first available CSV file
        works_file = None
        for csv_option in self.WORKS_CSV_OPTIONS:
            candidate = self.cache_dir / csv_option
            if candidate.exists():
                works_file = candidate
                break

        if not works_file:
            print(f"Warning: Could not find works CSV in {self.cache_dir}")
            print(f"Available files: {list(self.cache_dir.glob('*.csv'))}")
            return []

        print(f"Loading PPORTAL works from {works_file}...")

        try:
            works = []
            with open(works_file, "r", encoding="utf-8", errors="replace") as f:
                # Detect delimiter - try tab first (most common in this dataset)
                sample = f.read(1024)
                f.seek(0)

                delimiter = "\t"  # Default to tab
                if "|" in sample and sample.count("|") > sample.count("\t"):
                    delimiter = "|"
                elif "," in sample and sample.count(",") > sample.count("\t"):
                    delimiter = ","

                reader = csv.DictReader(f, delimiter=delimiter)
                for row in reader:
                    works.append(dict(row))

            print(f"  Loaded {len(works)} works")
            self._works_data = works
            return works

        except Exception as e:
            print(f"Error loading PPORTAL data: {e}")
            return []

    def _parse_work_row(self, row: Dict) -> Optional[Dict]:
        """Parse a work row from CSV into standardized metadata."""
        # Handle different CSV formats
        work_id = row.get("original_id") or row.get("id")
        title = row.get("work_title") or row.get("title")
        author = row.get("work_authors") or row.get("author") or row.get("work_authors")

        if not work_id or not title:
            return None

        # Clean up title and author
        title = title.strip()
        author = author.strip() if author else None

        # Get other metadata
        file_format = row.get("file_format", "")
        file_size = row.get("file_size", "")
        access_count = row.get("number_of_access", "")
        original_source = row.get("original_source", "")
        download_link = row.get("download_link", "")

        # Build metadata
        meta = {
            "source": self.source_name,
            "book_id": str(work_id),
            "title": title,
            "author": author,
            "language": "pt",  # Portuguese
            "category": original_source if original_source else None,
            "copyright_status": "Public Domain",
            "url": download_link if download_link else self.get_book_url(str(work_id)),
            "files": [],
            "format": file_format if file_format else None,
            "file_size": file_size if file_size else None,
        }

        return meta

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

        if count > 0:
            print(f"Iterated {count} works from PPORTAL")

    def extract_metadata(self, book_id: str) -> Dict[str, object]:
        """Extract metadata for a single work by ID."""
        works = self._load_works_data()

        # Find work by ID
        for row in works:
            work_id = row.get("original_id") or row.get("id")
            if str(work_id) == str(book_id):
                meta = self._parse_work_row(row)
                if meta:
                    return meta
                break

        # Return minimal metadata if not found
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
        return f"https://zenodo.org/record/5178063"

    def supports_downloads(self) -> bool:
        """PPORTAL provides metadata only, not full text."""
        return False

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

            title = (row.get("work_title") or row.get("title") or "").lower()
            author = (row.get("work_authors") or row.get("author") or "").lower()

            if query_lower in title or query_lower in author:
                work_id = row.get("original_id") or row.get("id")
                download_link = row.get("download_link", "")
                results.append(
                    {
                        "source": self.source_name,
                        "book_id": str(work_id),
                        "title": row.get("work_title") or row.get("title"),
                        "author": row.get("work_authors") or row.get("author"),
                        "url": download_link
                        if download_link
                        else self.get_book_url(str(work_id)),
                    }
                )

        return results

    def get_dataset_info(self) -> Dict:
        """Get information about the loaded dataset."""
        works = self._load_works_data()

        return {
            "total_works": len(works),
            "cache_dir": str(self.cache_dir),
            "dataset_url": self.ZENODO_URL,
        }
