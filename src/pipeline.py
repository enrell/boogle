import argparse
from pathlib import Path

from src.downloader.downloader import EpubDownloader
from src.parser.parser import EpubParser
from src.indexer.bm25 import BM25Index


def download_corpus(output_dir: str, limit: int | None = None, batch_size: int = 100):
    downloader = EpubDownloader(output_dir=output_dir)
    total = downloader.download_all(limit=limit, batch_size=batch_size)
    print(f"Downloaded {total} epubs")
    return total


def index_corpus(
    epub_dir: str,
    index_path: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 100
):
    epub_dir = Path(epub_dir)
    parser = EpubParser(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    index = BM25Index()
    doc_id = 0
    processed = 0
    
    epub_files = list(epub_dir.glob("*.epub"))
    total_files = len(epub_files)
    
    for i, epub_file in enumerate(epub_files):
        book_id = epub_file.stem
        for chunk in parser.process_epub(epub_file):
            metadata = {'book_id': book_id, 'chunk_id': doc_id}
            index.add_document(doc_id, chunk, metadata)
            doc_id += 1
        processed += 1
        if processed % 100 == 0:
            print(f"Indexed {processed}/{total_files} books, {doc_id} chunks")
    
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
    
    search_parser = subparsers.add_parser('search')
    search_parser.add_argument('query')
    search_parser.add_argument('--index-path', default='data/bm25.index')
    search_parser.add_argument('--top-k', type=int, default=10)
    
    args = parser.parse_args()
    
    if args.command == 'download':
        download_corpus(args.output, args.limit, args.batch_size)
    elif args.command == 'index':
        index_corpus(args.epub_dir, args.index_path, args.chunk_size, args.chunk_overlap)
    elif args.command == 'search':
        index = BM25Index.load(args.index_path)
        results = index.search(args.query, args.top_k)
        for doc_id, score, meta in results:
            print(f"[{score:.4f}] book={meta.get('book_id')} chunk={meta.get('chunk_id')}")


if __name__ == '__main__':
    main()
