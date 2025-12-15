import time
from pathlib import Path
from typing import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests

from src.scraper.scraper import GutenbergScraper


_local = threading.local()


def _get_session() -> requests.Session:
    if not hasattr(_local, 'session'):
        _local.session = requests.Session()
        _local.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    return _local.session


class EpubDownloader:
    def __init__(self, output_dir: str = "data/epubs", max_workers: int = 16):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.base_url = "https://www.gutenberg.org"
        self.scraper = GutenbergScraper()
        self.max_workers = max_workers

    def get_epub_url(self, book_id: str) -> str:
        return f"{self.base_url}/ebooks/{book_id}.epub.noimages"

    def download_epub(self, book_id: str) -> Path | None:
        filepath = self.output_dir / f"{book_id}.epub"
        if filepath.exists():
            return filepath
        url = self.get_epub_url(book_id)
        try:
            session = _get_session()
            resp = session.get(url, timeout=30, allow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 0:
                filepath.write_bytes(resp.content)
                return filepath
        except requests.RequestException:
            pass
        return None

    def download_batch(self, book_ids: list[str]) -> dict[str, Path]:
        results = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(self.download_epub, bid): bid for bid in book_ids}
            for future in as_completed(futures):
                bid = futures[future]
                try:
                    path = future.result()
                    if path:
                        results[bid] = path
                except Exception:
                    pass
        return results

    def iter_all_book_ids(self, limit: int | None = None) -> Iterator[str]:
        yield from self.scraper.iter_book_ids(limit=limit)

    def download_all(self, limit: int | None = None, batch_size: int = 200) -> int:
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
                results = self.download_batch(batch)
                for bid in results:
                    downloaded_ids.add(bid)
                checkpoint_file.write_text("\n".join(downloaded_ids))
                total += len(results)
                print(f"Downloaded {total} epubs")
                batch = []
        
        if batch:
            results = self.download_batch(batch)
            for bid in results:
                downloaded_ids.add(bid)
            checkpoint_file.write_text("\n".join(downloaded_ids))
            total += len(results)
        
        return total
