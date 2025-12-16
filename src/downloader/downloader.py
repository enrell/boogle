import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator

import requests
from bs4 import BeautifulSoup

from src.db.database import PostgresRepository
from src.scraper.scraper import GutenbergScraper

_local = threading.local()

# Format priority: plain text > epub.noimages > epub > pdf
FORMAT_PRIORITY = [
    ("txt", ".txt.utf-8", "text/plain"),
    ("txt", ".txt", "text/plain"),
    ("epub", ".epub.noimages", "application/epub+zip"),
    ("epub", ".epub.images", "application/epub+zip"),
    ("pdf", ".pdf", "application/pdf"),
]


def _get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
    return _local.session


def _fetch_metadata(book_id: str) -> dict:
    url = f"https://www.gutenberg.org/ebooks/{book_id}"
    meta = {"book_id": book_id, "source": "gutenberg", "url": url}
    try:
        session = _get_session()
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return meta

        soup = BeautifulSoup(resp.text, "html.parser")

        title_elem = soup.find("td", itemprop="headline")
        if title_elem:
            meta["title"] = title_elem.get_text(strip=True)

        bibrec = soup.find("table", class_="bibrec")
        if bibrec:
            for row in bibrec.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                key = th.get_text(strip=True).lower()
                if key == "author":
                    link = td.find("a")
                    meta["author"] = link.get_text(strip=True) if link else td.get_text(strip=True)
                elif key == "illustrator":
                    link = td.find("a")
                    meta["illustrator"] = link.get_text(strip=True) if link else td.get_text(strip=True)
                elif key == "language":
                    meta["language"] = td.get_text(strip=True)
                elif key == "category":
                    meta["category"] = td.get_text(strip=True)
                elif key == "release date":
                    meta["release_date"] = td.get_text(strip=True)
                elif key == "copyright status":
                    meta["copyright_status"] = td.get_text(strip=True)

        return meta
    except Exception:
        return meta


def _download_book(book_id: str, output_dir: Path, log_file: Path) -> tuple[str, Path | None, dict, str | None]:
    """Download book in best available format. Returns (book_id, path, meta, format_type)."""
    session = _get_session()
    base_url = f"https://www.gutenberg.org/ebooks/{book_id}"
    
    for fmt_type, suffix, _ in FORMAT_PRIORITY:
        ext = ".txt" if fmt_type == "txt" else f".{fmt_type}"
        filepath = output_dir / f"{book_id}{ext}"
        
        if filepath.exists():
            meta = _fetch_metadata(book_id)
            meta["format"] = fmt_type
            return book_id, filepath, meta, fmt_type
        
        url = f"{base_url}{suffix}"
        try:
            resp = session.get(url, timeout=30, allow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 100:
                filepath.write_bytes(resp.content)
                meta = _fetch_metadata(book_id)
                meta["format"] = fmt_type
                return book_id, filepath, meta, fmt_type
        except requests.RequestException:
            continue
    
    # No format available - log it
    meta = _fetch_metadata(book_id)
    log_entry = {
        "book_id": book_id,
        "url": base_url,
        "title": meta.get("title"),
        "reason": "no_supported_format",
    }
    with open(log_file, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    
    return book_id, None, meta, None


class BookSeeder:
    def __init__(self, output_dir: str = "data/books", max_workers: int = 16):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.output_dir / "skipped.jsonl"
        self.scraper = GutenbergScraper()
        self.max_workers = max_workers
        self.db = PostgresRepository()

    def iter_all_book_ids(self, limit: int | None = None) -> Iterator[str]:
        yield from self.scraper.iter_book_ids(limit=limit)

    def seed_all(self, limit: int | None = None, batch_size: int = 200) -> int:
        checkpoint_file = self.output_dir / ".checkpoint"

        downloaded_ids = set()
        if checkpoint_file.exists():
            downloaded_ids = set(checkpoint_file.read_text().splitlines())

        total = 0
        batch = []

        for book_id in self.iter_all_book_ids(limit=limit):
            if book_id in downloaded_ids:
                continue
            batch.append(book_id)

            if len(batch) >= batch_size:
                results = self._process_batch(batch)
                for bid, path, meta, fmt in results:
                    if path:
                        downloaded_ids.add(bid)
                        total += 1
                    self.db.upsert_book(meta)

                checkpoint_file.write_text("\n".join(downloaded_ids))
                print(f"Seeded {total} books")
                batch = []

        if batch:
            results = self._process_batch(batch)
            for bid, path, meta, fmt in results:
                if path:
                    downloaded_ids.add(bid)
                    total += 1
                self.db.upsert_book(meta)
            checkpoint_file.write_text("\n".join(downloaded_ids))

        return total

    def _process_batch(self, book_ids: list[str]) -> list[tuple[str, Path | None, dict, str | None]]:
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(_download_book, bid, self.output_dir, self.log_file): bid
                for bid in book_ids
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception:
                    pass
        return results


# Backward compatibility
EpubSeeder = BookSeeder
