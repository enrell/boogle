import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator

import requests

from src.db.database import PostgresRepository
from src.scraper.scraper import GutenbergScraper

_local = threading.local()

FORMAT_PRIORITY = [
    ("txt", ".txt.utf-8"),
    ("txt", ".txt"),
    ("epub", ".epub.noimages"),
    ("epub", ".epub.images"),
    ("pdf", ".pdf"),
]


def _get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
    return _local.session


def _get_scraper() -> GutenbergScraper:
    if not hasattr(_local, "scraper"):
        _local.scraper = GutenbergScraper()
    return _local.scraper


def _fetch_metadata(book_id: str) -> dict:
    try:
        return _get_scraper().extract_metadata(book_id)
    except Exception:
        return {"book_id": book_id, "source": "gutenberg", "url": f"https://www.gutenberg.org/ebooks/{book_id}"}


def _download_book(book_id: str, output_dir: Path, log_file: Path, pre_meta: dict | None = None) -> tuple[str, Path | None, dict, str | None]:
    session = _get_session()
    base_url = f"https://www.gutenberg.org/ebooks/{book_id}"
    
    # Use pre_meta if available to avoid scraping
    meta = pre_meta.copy() if pre_meta else _fetch_metadata(book_id)
    
    for fmt_type, suffix in FORMAT_PRIORITY:
        ext = ".txt" if fmt_type == "txt" else f".{fmt_type}"
        filepath = output_dir / f"{book_id}{ext}"
        
        if filepath.exists():
            meta["format"] = fmt_type
            return book_id, filepath, meta, fmt_type
        
        url = f"{base_url}{suffix}"
        try:
            resp = session.get(url, timeout=30, allow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 100:
                filepath.write_bytes(resp.content)
                meta["format"] = fmt_type
                return book_id, filepath, meta, fmt_type
        except requests.RequestException:
            continue
    
    # Final check if we really need to scrape for metadata (fallback)
    if not pre_meta:
        meta = _fetch_metadata(book_id)

    log_entry = {"book_id": book_id, "url": base_url, "title": meta.get("title"), "reason": "no_supported_format"}
    with open(log_file, "a") as f:
        f.write(json.dumps(log_entry) + "\n")
    
    return book_id, None, meta, None


class BookSeeder:
    def __init__(self, output_dir: str = "data/books", max_workers: int = 16, use_sqlite: bool = False):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.output_dir / "skipped.jsonl"
        self.scraper = GutenbergScraper()
        self.max_workers = max_workers
        self.db = PostgresRepository(use_sqlite=use_sqlite)

    def iter_all_books(self, limit: int | None = None) -> Iterator[dict]:
        yield from self.scraper.iter_book_metadata(limit=limit)


    def _filter_books(self, batch: list[dict]) -> list[dict]:
        """Skip Dictionaries, Encyclopedias, and other super-documents."""
        STOP_WORDS = {
            "dictionary", "encyclopedia", "thesaurus", "full text", 
            "complete works", "webster's", "unabridged"
        }
        filtered = []
        for meta in batch:
            title = (meta.get("title") or "").lower()
            if any(w in title for w in STOP_WORDS):
                continue
            filtered.append(meta)
        return filtered

    def seed_all(self, limit: int | None = None, batch_size: int = 500) -> int:
        checkpoint_file = self.output_dir / ".checkpoint"

        downloaded_ids = set()
        if checkpoint_file.exists():
            downloaded_ids = set(checkpoint_file.read_text().splitlines())

        total = 0
        batch = []

        for meta in self.iter_all_books(limit=limit):
            book_id = meta['book_id']
            if book_id in downloaded_ids:
                continue
            batch.append(meta)

            if len(batch) >= batch_size:
                filtered_batch = self._filter_books(batch)
                results = self._process_batch(filtered_batch)
                for bid, path, meta_res, fmt in results:
                    if path:
                        downloaded_ids.add(bid)
                        total += 1
                    self.db.upsert_book(meta_res)
                
                skipped_ids = {m['book_id'] for m in batch} - {m['book_id'] for m in filtered_batch}
                downloaded_ids.update(skipped_ids)

                checkpoint_file.write_text("\n".join(downloaded_ids))
                print(f"Seeded {total} books (skipped {len(skipped_ids)} super-documents)")
                batch = []

        if batch:
            filtered_batch = self._filter_books(batch)
            results = self._process_batch(filtered_batch)
            for bid, path, meta_res, fmt in results:
                if path:
                    downloaded_ids.add(bid)
                    total += 1
                self.db.upsert_book(meta_res)
            
            skipped_ids = {m['book_id'] for m in batch} - {m['book_id'] for m in filtered_batch}
            downloaded_ids.update(skipped_ids)
            
            checkpoint_file.write_text("\n".join(downloaded_ids))

        return total

    def update_metadata(self, batch_size: int = 100) -> int:
        checkpoint_file = self.output_dir / ".checkpoint"
        if not checkpoint_file.exists():
            return 0

        book_ids = checkpoint_file.read_text().splitlines()
        total = len(book_ids)
        updated = 0

        for i in range(0, total, batch_size):
            batch = book_ids[i:i + batch_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(_fetch_metadata, bid): bid for bid in batch}
                for future in as_completed(futures):
                    try:
                        meta = future.result()
                        self.db.upsert_book(meta)
                        updated += 1
                    except Exception:
                        pass
            print(f"Updated {updated}/{total} books")

        return updated

    def _process_batch(self, batch_meta: list[dict]) -> list[tuple[str, Path | None, dict, str | None]]:
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(_download_book, m['book_id'], self.output_dir, self.log_file, m): m
                for m in batch_meta
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception:
                    pass
        return results


# Backward compatibility
EpubSeeder = BookSeeder
