import argparse
import os
from pathlib import Path

from rust_bm25 import index_corpus_file, FileSearcher

from src.downloader.downloader import BookSeeder
from src.indexer.stopwords import load_stopwords


def seed_corpus(output_dir: str, limit: int | None = None, batch_size: int = 200, workers: int = 16, refresh: bool = False, use_sqlite: bool = False):
    seeder = BookSeeder(output_dir=output_dir, max_workers=workers, use_sqlite=use_sqlite)
    if refresh:
        updated = seeder.update_metadata(batch_size=batch_size)
        print(f"Refreshed {updated} books")
    total = seeder.seed_all(limit=limit, batch_size=batch_size)
    print(f"Seeded {total} books")
    return total


def update_metadata(output_dir: str, batch_size: int = 100, workers: int = 16, use_sqlite: bool = False):
    seeder = BookSeeder(output_dir=output_dir, max_workers=workers, use_sqlite=use_sqlite)
    total = seeder.update_metadata(batch_size=batch_size)
    print(f"Updated {total} books")
    return total


def index_corpus(
    books_dir: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 100,
    full: bool = False,
    batch_size: int = 1000,
):
    index_dir = os.getenv("INDEX_DIR", "data/index")
    chunks_dir = os.getenv("CHUNKS_DIR", "data/chunks")
    stopwords = list(load_stopwords())
    
    Path(index_dir).mkdir(parents=True, exist_ok=True)
    Path(chunks_dir).mkdir(parents=True, exist_ok=True)
    
    if full:
        import shutil
        if Path(index_dir).exists():
            shutil.rmtree(index_dir)
        Path(index_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"Indexing to filesystem: {index_dir}")
    indexed, total_chunks = index_corpus_file(
        books_dir,
        index_dir,
        chunks_dir,
        stopwords,
        chunk_size,
        chunk_overlap,
        batch_size,
    )
    
    return indexed, total_chunks


def search(query: str, top_k: int = 10, use_sqlite: bool = False):
    from src.db.database import PostgresRepository
    
    index_dir = os.getenv("INDEX_DIR", "data/index")
    stopwords = list(load_stopwords())
    
    searcher = FileSearcher(index_dir)
    searcher.set_stopwords(stopwords)
    
    results = searcher.search(query, top_k * 10)
    
    db = PostgresRepository(use_sqlite=use_sqlite)
    seen_books = set()
    count = 0
    
    for book_id, score, chunk_id in results:
        if book_id in seen_books:
            continue
        seen_books.add(book_id)
        
        meta = db.get_book("gutenberg", book_id)
        if meta:
            title = meta.get("title", "Unknown")
            author = meta.get("author", "Unknown")
            print(f"[{score:.4f}] {title} by {author} (book={book_id})")
            count += 1
            if count >= top_k:
                break


def run_api(host: str = "0.0.0.0", port: int = 8000, use_sqlite: bool = False):
    import uvicorn
    
    if use_sqlite:
        os.environ["USE_SQLITE"] = "1"
        print("Starting API in SQLite mode (data/boogle.db)")
    else:
        print("Starting API in PostgreSQL mode")
        
    uvicorn.run("src.api.main:app", host=host, port=port, reload=True)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    seed_parser = subparsers.add_parser('seed')
    seed_parser.add_argument('--output', default='data/books')
    seed_parser.add_argument('--limit', type=int, default=None)
    seed_parser.add_argument('--batch-size', type=int, default=1000)
    seed_parser.add_argument('--workers', type=int, default=16)
    seed_parser.add_argument('--refresh', action='store_true')
    seed_parser.add_argument('--sqlite', action='store_true', help='Use SQLite instead of PostgreSQL')
    
    update_parser = subparsers.add_parser('update-metadata')
    update_parser.add_argument('--output', default='data/books')
    update_parser.add_argument('--batch-size', type=int, default=1000)
    update_parser.add_argument('--workers', type=int, default=16)
    update_parser.add_argument('--sqlite', action='store_true', help='Use SQLite instead of PostgreSQL')
    
    idx_parser = subparsers.add_parser('index')
    idx_parser.add_argument('--books-dir', default='data/books')
    idx_parser.add_argument('--chunk-size', type=int, default=1000)
    idx_parser.add_argument('--chunk-overlap', type=int, default=100)
    idx_parser.add_argument('--batch-size', type=int, default=1000)
    idx_parser.add_argument('--full', action='store_true', help='Full reindex (clear existing)')
    
    search_parser = subparsers.add_parser('search')
    search_parser.add_argument('query')
    search_parser.add_argument('--top-k', type=int, default=10)
    search_parser.add_argument('--sqlite', action='store_true', help='Use SQLite instead of PostgreSQL')
    
    api_parser = subparsers.add_parser('api')
    api_parser.add_argument('--host', default="0.0.0.0")
    api_parser.add_argument('--port', type=int, default=8000)
    api_parser.add_argument('--sqlite', action='store_true', help='Use SQLite instead of PostgreSQL')
    
    args = parser.parse_args()
    
    if args.command == 'seed':
        seed_corpus(args.output, args.limit, args.batch_size, args.workers, args.refresh, args.sqlite)
    elif args.command == 'update-metadata':
        update_metadata(args.output, args.batch_size, args.workers, args.sqlite)
    elif args.command == 'index':
        index_corpus(args.books_dir, args.chunk_size, args.chunk_overlap, args.full, args.batch_size)
    elif args.command == 'search':
        search(args.query, args.top_k, args.sqlite)
    elif args.command == 'api':
        run_api(args.host, args.port, args.sqlite)


if __name__ == '__main__':
    main()

