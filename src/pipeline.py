import argparse
import os
import shutil
from pathlib import Path

from rust_bm25 import index_corpus_file, FileSearcher

from src.downloader.downloader import BookSeeder
from src.indexer.stopwords import load_stopwords
from src.db.database import PostgresRepository


def run_index_pipeline(
    limit: int | None = None,
    batch_size: int = 1000,
    use_sqlite: bool = False,
    reindex: bool = False,
    chunk_size: int = 1000,
    chunk_overlap: int = 100,
    workers: int = 16,
):
    """
    Runs the complete indexing pipeline:
    1. Download/Seed books (incremental/persistent).
    2. Update Metadata in DB (Postgres or SQLite).
    3. Build BM25 Index (full re-index of available files).
    """
    books_dir = "data/books"
    index_dir = os.getenv("INDEX_DIR", "data/index")
    chunks_dir = os.getenv("CHUNKS_DIR", "data/chunks")
    
    print(f"--- Step 1: Seeding Corpus (SQLite={use_sqlite}) ---")
    seeder = BookSeeder(output_dir=books_dir, max_workers=workers, use_sqlite=use_sqlite)
    seeded_total = seeder.seed_all(limit=limit, batch_size=batch_size)
    print(f"Seeding complete. Total new/verified books in this run: {seeded_total}")

    print(f"\n--- Step 2: Building Index (Reindex={reindex}) ---")
    Path(index_dir).mkdir(parents=True, exist_ok=True)
    Path(chunks_dir).mkdir(parents=True, exist_ok=True)

    if reindex:
        print(f"Clearing existing index at {index_dir}...")
        if Path(index_dir).exists():
            shutil.rmtree(index_dir)
        Path(index_dir).mkdir(parents=True, exist_ok=True)

    stopwords = list(load_stopwords())
    
    print(f"Indexing files from {books_dir} to {index_dir}...")
    indexed, total_chunks = index_corpus_file(
        books_dir,
        index_dir,
        chunks_dir,
        stopwords,
        chunk_size,
        chunk_overlap,
        batch_size,
    )
    
    print(f"Indexing complete. Processed {indexed} books into {total_chunks} chunks.")
    return indexed


def search(query: str, top_k: int = 10, use_sqlite: bool = False):
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
    parser = argparse.ArgumentParser(description="Boogle Search Pipeline CLI")
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    idx_parser = subparsers.add_parser('index', help='Seed corpus and build index')
    idx_parser.add_argument('--limit', type=int, default=None, help='Limit number of books to seed')
    idx_parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    idx_parser.add_argument('--sqlite', action='store_true', help='Use SQLite instead of PostgreSQL')
    idx_parser.add_argument('--reindex', action='store_true', help='Force full re-index (clear existing index)')
    idx_parser.add_argument('--workers', type=int, default=16, help='Number of worker threads for seeding')
    idx_parser.add_argument('--chunk-size', type=int, default=1000, help='Text chunk size')
    idx_parser.add_argument('--chunk-overlap', type=int, default=100, help='Text chunk overlap')
    
    search_parser = subparsers.add_parser('search', help='Search the index')
    search_parser.add_argument('query', help='Search query')
    search_parser.add_argument('--top-k', type=int, default=10, help='Number of results')
    search_parser.add_argument('--sqlite', action='store_true', help='Use SQLite instead of PostgreSQL')
    
    api_parser = subparsers.add_parser('api', help='Run the REST API')
    api_parser.add_argument('--host', default="0.0.0.0")
    api_parser.add_argument('--port', type=int, default=8000)
    api_parser.add_argument('--sqlite', action='store_true', help='Use SQLite instead of PostgreSQL')
    
    args = parser.parse_args()
    
    if args.command == 'index':
        run_index_pipeline(
            limit=args.limit,
            batch_size=args.batch_size,
            use_sqlite=args.sqlite,
            reindex=args.reindex,
            chunk_size=args.chunk_size,
            chunk_overlap=args.chunk_overlap,
            workers=args.workers
        )
    elif args.command == 'search':
        search(args.query, args.top_k, args.sqlite)
    elif args.command == 'api':
        run_api(args.host, args.port, args.sqlite)


if __name__ == '__main__':
    main()

