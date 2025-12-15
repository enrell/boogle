import argparse
import json
from pathlib import Path

from rust_bm25 import parse_epubs_batch

from src.downloader.downloader import EpubDownloader
from src.scraper.scraper import GutenbergScraper
from src.indexer.pg_bm25 import PgBM25Index


def download_corpus(output_dir: str, limit: int | None = None, batch_size: int = 200, workers: int = 16):
    downloader = EpubDownloader(output_dir=output_dir, max_workers=workers)
    total = downloader.download_all(limit=limit, batch_size=batch_size)
    print(f"Downloaded {total} epubs")
    return total


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
    fetch_metadata: bool = False,
    flush_every: int = 20000,
    batch_size: int = 100
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
    
    for i in range(0, total_files, batch_size):
        batch_files = epub_files[i:i + batch_size]
        paths = [str(f) for f in batch_files]
        
        results = parse_epubs_batch(paths, chunk_size, chunk_overlap)
        
        for book_id, chunks in results:
            if fetch_metadata and book_id not in metadata_cache:
                metadata_cache[book_id] = _fetch_metadata(book_id)
            
            book_meta = metadata_cache.get(book_id, {'book_id': book_id})
            
            for chunk in chunks:
                meta = {**book_meta, 'chunk_id': doc_id}
                index.add_document(doc_id, chunk, json.dumps(meta))
                doc_id += 1
            
            processed += 1
        
        print(f"Indexed {processed}/{total_files} books, {doc_id} chunks")
    
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
    dl_parser.add_argument('--batch-size', type=int, default=200)
    dl_parser.add_argument('--workers', type=int, default=16)
    
    idx_parser = subparsers.add_parser('index')
    idx_parser.add_argument('--epub-dir', default='data/epubs')
    idx_parser.add_argument('--chunk-size', type=int, default=1000)
    idx_parser.add_argument('--chunk-overlap', type=int, default=100)
    idx_parser.add_argument('--fetch-metadata', action='store_true')
    idx_parser.add_argument('--flush-every', type=int, default=20000)
    idx_parser.add_argument('--batch-size', type=int, default=100)
    
    search_parser = subparsers.add_parser('search')
    search_parser.add_argument('query')
    search_parser.add_argument('--top-k', type=int, default=10)
    
    args = parser.parse_args()
    
    if args.command == 'download':
        download_corpus(args.output, args.limit, args.batch_size, args.workers)
    elif args.command == 'index':
        index_corpus(
            args.epub_dir, args.chunk_size, args.chunk_overlap,
            args.fetch_metadata, args.flush_every, args.batch_size
        )
    elif args.command == 'search':
        search(args.query, args.top_k)


if __name__ == '__main__':
    main()
