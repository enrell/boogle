import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from threading import Thread

from src.downloader.downloader import EpubDownloader
from src.parser.parser import EpubParser
from src.scraper.scraper import GutenbergScraper
from src.indexer.pg_bm25 import PgBM25Index


def download_corpus(output_dir: str, limit: int | None = None, batch_size: int = 100, workers: int = 8):
    downloader = EpubDownloader(output_dir=output_dir, max_workers=workers)
    total = downloader.download_all(limit=limit, batch_size=batch_size)
    print(f"Downloaded {total} epubs")
    return total


def _parse_epub(args: tuple) -> list[tuple[str, str]]:
    filepath, chunk_size, chunk_overlap = args
    parser = EpubParser(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    book_id = Path(filepath).stem
    return [(book_id, chunk) for chunk in parser.process_epub(filepath)]


def _fetch_metadata(book_id: str) -> dict:
    scraper = GutenbergScraper()
    try:
        meta = scraper.extract_metadata(book_id)
        del meta['files']
        return meta
    except Exception:
        return {'book_id': book_id, 'source': 'gutenberg'}


def index_corpus(
    epub_dir: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 100,
    workers: int = 4,
    fetch_metadata: bool = False,
    flush_every: int = 5000
):
    epub_dir = Path(epub_dir)
    epub_files = list(epub_dir.glob("*.epub"))
    total_files = len(epub_files)
    
    metadata_cache: dict[str, dict] = {}
    metadata_file = epub_dir / "metadata.json"
    if metadata_file.exists():
        metadata_cache = json.loads(metadata_file.read_text())
    
    index = PgBM25Index(flush_every=flush_every)
    index.clear()
    
    doc_id = 0
    processed = 0
    queue: Queue = Queue(maxsize=workers * 2)
    
    def producer():
        tasks = [(str(f), chunk_size, chunk_overlap) for f in epub_files]
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for result in executor.map(_parse_epub, tasks):
                queue.put(result)
        queue.put(None)
    
    producer_thread = Thread(target=producer)
    producer_thread.start()
    
    while True:
        results = queue.get()
        if results is None:
            break
        
        if not results:
            processed += 1
            continue
        
        book_id = results[0][0]
        
        if fetch_metadata and book_id not in metadata_cache:
            metadata_cache[book_id] = _fetch_metadata(book_id)
            if processed % 100 == 0:
                metadata_file.write_text(json.dumps(metadata_cache))
        
        book_meta = metadata_cache.get(book_id, {'book_id': book_id})
        
        for _, chunk in results:
            meta = {**book_meta, 'chunk_id': doc_id}
            index.add_document(doc_id, chunk, json.dumps(meta))
            doc_id += 1
        
        processed += 1
        if processed % 50 == 0:
            print(f"Indexed {processed}/{total_files} books, {doc_id} chunks")
    
    producer_thread.join()
    
    if fetch_metadata:
        metadata_file.write_text(json.dumps(metadata_cache))
    
    index.finalize()
    print(f"Index saved: {doc_id} chunks from {processed} books")


def search(query: str, top_k: int = 10):
    index = PgBM25Index()
    results = index.search(query, top_k)
    for doc_id, score, meta in results:
        title = meta.get('title', 'Unknown')
        author = meta.get('author', 'Unknown')
        print(f"[{score:.4f}] {title} by {author} (book={meta.get('book_id')}, chunk={meta.get('chunk_id')})")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    dl_parser = subparsers.add_parser('download')
    dl_parser.add_argument('--output', default='data/epubs')
    dl_parser.add_argument('--limit', type=int, default=None)
    dl_parser.add_argument('--batch-size', type=int, default=100)
    dl_parser.add_argument('--workers', type=int, default=8)
    
    idx_parser = subparsers.add_parser('index')
    idx_parser.add_argument('--epub-dir', default='data/epubs')
    idx_parser.add_argument('--chunk-size', type=int, default=1000)
    idx_parser.add_argument('--chunk-overlap', type=int, default=100)
    idx_parser.add_argument('--workers', type=int, default=4)
    idx_parser.add_argument('--fetch-metadata', action='store_true')
    idx_parser.add_argument('--flush-every', type=int, default=5000)
    
    search_parser = subparsers.add_parser('search')
    search_parser.add_argument('query')
    search_parser.add_argument('--top-k', type=int, default=10)
    
    args = parser.parse_args()
    
    if args.command == 'download':
        download_corpus(args.output, args.limit, args.batch_size, args.workers)
    elif args.command == 'index':
        index_corpus(
            args.epub_dir, args.chunk_size, args.chunk_overlap,
            args.workers, args.fetch_metadata, args.flush_every
        )
    elif args.command == 'search':
        search(args.query, args.top_k)


if __name__ == '__main__':
    main()
