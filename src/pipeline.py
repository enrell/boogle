import argparse
import os
from pathlib import Path

from rust_bm25 import index_corpus as rust_index_corpus

from src.downloader.downloader import BookSeeder
from src.indexer.stopwords import load_stopwords


def seed_corpus(output_dir: str, limit: int | None = None, batch_size: int = 200, workers: int = 16, refresh: bool = False):
    seeder = BookSeeder(output_dir=output_dir, max_workers=workers)
    if refresh:
        updated = seeder.update_metadata(batch_size=batch_size)
        print(f"Refreshed {updated} books")
    total = seeder.seed_all(limit=limit, batch_size=batch_size)
    print(f"Seeded {total} books")
    return total


def update_metadata(output_dir: str, batch_size: int = 100, workers: int = 16):
    seeder = BookSeeder(output_dir=output_dir, max_workers=workers)
    total = seeder.update_metadata(batch_size=batch_size)
    print(f"Updated {total} books")
    return total


def get_db_url() -> str:
    """Build database URL from environment variables."""
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "boogle")
    password = os.getenv("POSTGRES_PASSWORD", "boogle")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "boogle")
    return f"host={host} port={port} user={user} password={password} dbname={database}"


def index_corpus(
    books_dir: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 100,
    full: bool = False,
    batch_size: int = 100,
):
    """Index books using the Rust implementation."""
    chunks_dir = os.getenv("CHUNKS_DIR", "data/chunks")
    db_url = get_db_url()
    stopwords = list(load_stopwords())
    
    # Ensure chunks directory exists
    Path(chunks_dir).mkdir(parents=True, exist_ok=True)
    
    # Call Rust function - does everything
    indexed, skipped, total_chunks = rust_index_corpus(
        books_dir,
        chunks_dir,
        db_url,
        stopwords,
        chunk_size,
        chunk_overlap,
        full,
        batch_size,
    )
    
    return indexed, skipped, total_chunks


def search(query: str, top_k: int = 10):
    from src.indexer.ranker import Ranker
    from src.indexer.storage import IndexStorage
    
    with IndexStorage() as storage:
        ranker = Ranker(storage)
        results = ranker.search(query, top_k)
        for r in results:
            print(f"[{r.score:.4f}] {r.title} by {r.author} (book={r.book_id})")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    seed_parser = subparsers.add_parser('seed')
    seed_parser.add_argument('--output', default='data/books')
    seed_parser.add_argument('--limit', type=int, default=None)
    seed_parser.add_argument('--batch-size', type=int, default=200)
    seed_parser.add_argument('--workers', type=int, default=16)
    seed_parser.add_argument('--refresh', action='store_true')
    
    update_parser = subparsers.add_parser('update-metadata')
    update_parser.add_argument('--output', default='data/books')
    update_parser.add_argument('--batch-size', type=int, default=100)
    update_parser.add_argument('--workers', type=int, default=16)
    
    idx_parser = subparsers.add_parser('index')
    idx_parser.add_argument('--books-dir', default='data/books')
    idx_parser.add_argument('--chunk-size', type=int, default=1000)
    idx_parser.add_argument('--chunk-overlap', type=int, default=100)
    idx_parser.add_argument('--batch-size', type=int, default=100)
    idx_parser.add_argument('--full', action='store_true', help='Full reindex (clear existing)')
    
    search_parser = subparsers.add_parser('search')
    search_parser.add_argument('query')
    search_parser.add_argument('--top-k', type=int, default=10)
    
    args = parser.parse_args()
    
    if args.command == 'seed':
        seed_corpus(args.output, args.limit, args.batch_size, args.workers, args.refresh)
    elif args.command == 'update-metadata':
        update_metadata(args.output, args.batch_size, args.workers)
    elif args.command == 'index':
        index_corpus(args.books_dir, args.chunk_size, args.chunk_overlap, args.full, args.batch_size)
    elif args.command == 'search':
        search(args.query, args.top_k)


if __name__ == '__main__':
    main()
