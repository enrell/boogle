import json
from pathlib import Path
from typing import Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import requests
from bs4 import BeautifulSoup

from src.scraper.scraper import GutenbergScraper


_local = threading.local()


def _get_session() -> requests.Session:
    if not hasattr(_local, 'session'):
        _local.session = requests.Session()
        _local.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    return _local.session


def _fetch_metadata_fast(book_id: str) -> dict:
    url = f"https://www.gutenberg.org/ebooks/{book_id}"
    try:
        session = _get_session()
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            return {'book_id': book_id, 'source': 'gutenberg'}
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        meta = {'book_id': book_id, 'source': 'gutenberg', 'url': url}
        
        title_elem = soup.find('td', itemprop='headline')
        if title_elem:
            meta['title'] = title_elem.get_text(strip=True)
        
        bibrec = soup.find('table', class_='bibrec')
        if bibrec:
            for row in bibrec.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if not th or not td:
                    continue
                key = th.get_text(strip=True).lower()
                if key == 'author':
                    link = td.find('a')
                    meta['author'] = link.get_text(strip=True) if link else td.get_text(strip=True)
                elif key == 'language':
                    meta['language'] = td.get_text(strip=True)
                elif key == 'category':
                    meta['category'] = td.get_text(strip=True)
        
        return meta
    except Exception:
        return {'book_id': book_id, 'source': 'gutenberg'}


def _download_and_fetch(book_id: str, output_dir: Path, fetch_meta: bool) -> tuple[str, Path | None, dict | None]:
    filepath = output_dir / f"{book_id}.epub"
    path = None
    meta = None
    
    if not filepath.exists():
        url = f"https://www.gutenberg.org/ebooks/{book_id}.epub.noimages"
        try:
            session = _get_session()
            resp = session.get(url, timeout=30, allow_redirects=True)
            if resp.status_code == 200 and len(resp.content) > 0:
                filepath.write_bytes(resp.content)
                path = filepath
        except requests.RequestException:
            pass
    else:
        path = filepath
    
    if fetch_meta:
        meta = _fetch_metadata_fast(book_id)
    
    return book_id, path, meta


class EpubDownloader:
    def __init__(self, output_dir: str = "data/epubs", max_workers: int = 16):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.scraper = GutenbergScraper()
        self.max_workers = max_workers

    def iter_all_book_ids(self, limit: int | None = None) -> Iterator[str]:
        yield from self.scraper.iter_book_ids(limit=limit)

    def download_all(self, limit: int | None = None, batch_size: int = 200, fetch_metadata: bool = False) -> tuple[int, dict]:
        checkpoint_file = self.output_dir / ".checkpoint"
        metadata_file = self.output_dir / "metadata.json"
        
        downloaded_ids = set()
        if checkpoint_file.exists():
            downloaded_ids = set(checkpoint_file.read_text().splitlines())
        
        metadata_cache = {}
        if metadata_file.exists():
            metadata_cache = json.loads(metadata_file.read_text())
        
        total = 0
        batch = []
        
        for book_id in self.iter_all_book_ids(limit=limit):
            if book_id in downloaded_ids:
                continue
            batch.append(book_id)
            
            if len(batch) >= batch_size:
                results = self._process_batch(batch, fetch_metadata)
                for bid, path, meta in results:
                    if path:
                        downloaded_ids.add(bid)
                        total += 1
                    if meta:
                        metadata_cache[bid] = meta
                
                checkpoint_file.write_text("\n".join(downloaded_ids))
                if fetch_metadata:
                    metadata_file.write_text(json.dumps(metadata_cache))
                print(f"Downloaded {total} epubs")
                batch = []
        
        if batch:
            results = self._process_batch(batch, fetch_metadata)
            for bid, path, meta in results:
                if path:
                    downloaded_ids.add(bid)
                    total += 1
                if meta:
                    metadata_cache[bid] = meta
            
            checkpoint_file.write_text("\n".join(downloaded_ids))
            if fetch_metadata:
                metadata_file.write_text(json.dumps(metadata_cache))
        
        return total, metadata_cache

    def _process_batch(self, book_ids: list[str], fetch_meta: bool) -> list[tuple[str, Path | None, dict | None]]:
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(_download_and_fetch, bid, self.output_dir, fetch_meta): bid 
                for bid in book_ids
            }
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception:
                    pass
        return results
