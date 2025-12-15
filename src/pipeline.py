import argparse
import gc
import json
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from src.downloader.downloader import EpubDownloader
from src.parser.parser import EpubParser
from src.scraper.scraper import GutenbergScraper

try:
    from rust_bm25 import BM25Index
except ImportError:
    from src.indexer.bm25 import BM25Index


def download_corpus(output_dir: str, limit: int | None = None, batch_size: int = 100):
    downloader = EpubDownloader(output_dir=output_dir)
    total = downloader.download_all(limit=limit, batch_size=batch_size)
    print(f"Downloaded {total} epubs")
    return total


def _process_epub(args: tuple) -> list[tuple[str, str]]:
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
    index_path: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 100,
    workers: int = 1,
    fetch_metadata: bool = False
):
    epub_dir = Path(epub_dir)
    epub_files = list(epub_dir.glob("*.epub"))
    total_files = len(epub_files)
    
    metadata_cache: dict[str, dict] = {}
    metadata_file = Path(epub_dir) / "metadata.json"
    if metadata_file.exists():
        metadata_cache = json.loads(metadata_file.read_text())
    
    index = BM25Index()
    doc_id = 0
    processed = 0
    batch_size = max(workers * 4, 50)
    
    for i in range(0, total_files, batch_size):
        batch = epub_files[i:i + batch_size]
        tasks = [(str(f), chunk_size, chunk_overlap) for f in batch]
        
        if workers == 1:
            results_batch = [_process_epub(t) for t in tasks]
        else:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                results_batch = list(executor.map(_process_epub, tasks))
        
        for results in results_batch:
            if not results:
                processed += 1
                continue
            book_id = results[0][0]
            
            if fetch_metadata and book_id not in metadata_cache:
                metadata_cache[book_id] = _fetch_metadata(book_id)
            
            book_meta = metadata_cache.get(book_id, {'book_id': book_id})
            
            for _, chunk in results:
                meta = {**book_meta, 'chunk_id': doc_id}
                index.add_document(doc_id, chunk, json.dumps(meta))
                doc_id += 1
            processed += 1
        
        if fetch_metadata:
            metadata_file.write_text(json.dumps(metadata_cache))
        
        del results_batch
        gc.collect()
        print(f"Indexed {processed}/{total_files} books, {doc_id} chunks")
    
    index.finalize()
    index.save(index_path)
    print(f"Index saved: {doc_id} chunks from {processed} books")
    return index


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    dl_parser = subparsers.add_parser('download')
    dl_parser.add_argument('--output', default='data/epubs')
    dl_parser.add_argument('--limit', type=int, default=None)
    dl_parser.add_argument('--batch-size', type=int, default=100)
    
    idx_parser = subparsers.add_parser('index')
    idx_parser.add_argument('--epub-dir', default='data/epubs')
    idx_parser.add_argument('--index-path', default='data/bm25.index')
    idx_parser.add_argument('--chunk-size', type=int, default=1000)
    idx_parser.add_argument('--chunk-overlap', type=int, default=100)
    idx_parser.add_argument('--workers', type=int, default=1)
    idx_parser.add_argument('--fetch-metadata', action='store_true')
    
    search_parser = subparsers.add_parser('search')
    search_parser.add_argument('query')
    search_parser.add_argument('--index-path', default='data/bm25.index')
    search_parser.add_argument('--top-k', type=int, default=10)
    
    args = parser.parse_args()
    
    if args.command == 'download':
        download_corpus(args.output, args.limit, args.batch_size)
    elif args.command == 'index':
        index_corpus(
            args.epub_dir, args.index_path, args.chunk_size, 
            args.chunk_overlap, args.workers, args.fetch_metadata
        )
    elif args.command == 'search':
        index = BM25Index.load(args.index_path)
        results = index.search(args.query, args.top_k)
        for doc_id, score, meta_str in results:
            meta = json.loads(meta_str) if meta_str else {}
            title = meta.get('title', 'Unknown')
            author = meta.get('author', 'Unknown')
            print(f"[{score:.4f}] {title} by {author} (book={meta.get('book_id')}, chunk={meta.get('chunk_id')})")


if __name__ == '__main__':
    main()
