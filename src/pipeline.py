import argparse
import json
from pathlib import Path

from rust_bm25 import process_epubs_to_index

from src.downloader.downloader import EpubSeeder
from src.indexer.storage import IndexStorage
from src.db.database import PostgresRepository


def seed_corpus(output_dir: str, limit: int | None = None, batch_size: int = 200, workers: int = 16):
    seeder = EpubSeeder(output_dir=output_dir, max_workers=workers)
    total = seeder.seed_all(limit=limit, batch_size=batch_size)
    print(f"Seeded {total} books")
    return total


def index_corpus(
    epub_dir: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 100,
    batch_size: int = 200
):
    epub_dir = Path(epub_dir)
    epub_files = list(epub_dir.glob("*.epub"))
    total_files = len(epub_files)
    
    db = PostgresRepository()
    storage = IndexStorage()
    storage.clear()
    
    total_docs = 0
    total_length = 0
    processed = 0
    
    for i in range(0, total_files, batch_size):
        batch_files = epub_files[i:i + batch_size]
        paths = []
        metadatas = []
        
        for f in batch_files:
            book_id = f.stem
            book_meta = db.get_book('gutenberg', book_id)
            if not book_meta:
                book_meta = {'book_id': book_id, 'source': 'gutenberg'}
            paths.append(str(f))
            metadatas.append(json.dumps(book_meta))
        
        docs, terms, batch_length = process_epubs_to_index(
            paths, metadatas, chunk_size, chunk_overlap
        )
        
        if docs:
            adjusted_docs = [(d[0] + total_docs, d[1], d[2]) for d in docs]
            adjusted_terms = []
            for term, df, postings_bytes in terms:
                from rust_bm25 import decode_postings, encode_postings
                postings = decode_postings(postings_bytes)
                adjusted = [(doc_id + total_docs, tf) for doc_id, tf in postings]
                adjusted_terms.append((term, df, encode_postings(adjusted)))
            
            storage.insert_documents_batch(adjusted_docs)
            storage.insert_terms_batch(adjusted_terms, merge=True)
            
            total_docs += len(docs)
            total_length += batch_length
        
        processed += len(batch_files)
        print(f"Indexed {processed}/{total_files} books, {total_docs} chunks")
    
    avgdl = total_length / total_docs if total_docs > 0 else 0
    storage.set_global("num_docs", str(total_docs))
    storage.set_global("avgdl", str(avgdl))
    storage.set_global("k1", "1.5")
    storage.set_global("b", "0.75")
    
    print(f"Index saved: {total_docs} chunks from {processed} books")


def search(query: str, top_k: int = 10):
    from src.indexer.pg_bm25 import PgBM25Index
    index = PgBM25Index()
    results = index.search(query, top_k)
    for doc_id, score, meta in results:
        title = meta.get('title', 'Unknown')
        author = meta.get('author', 'Unknown')
        print(f"[{score:.4f}] {title} by {author} (book={meta.get('book_id')}, chunk={meta.get('chunk_id')})")


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)
    
    seed_parser = subparsers.add_parser('seed')
    seed_parser.add_argument('--output', default='data/epubs')
    seed_parser.add_argument('--limit', type=int, default=None)
    seed_parser.add_argument('--batch-size', type=int, default=200)
    seed_parser.add_argument('--workers', type=int, default=16)
    
    idx_parser = subparsers.add_parser('index')
    idx_parser.add_argument('--epub-dir', default='data/epubs')
    idx_parser.add_argument('--chunk-size', type=int, default=1000)
    idx_parser.add_argument('--chunk-overlap', type=int, default=100)
    idx_parser.add_argument('--batch-size', type=int, default=200)
    
    search_parser = subparsers.add_parser('search')
    search_parser.add_argument('query')
    search_parser.add_argument('--top-k', type=int, default=10)
    
    args = parser.parse_args()
    
    if args.command == 'seed':
        seed_corpus(args.output, args.limit, args.batch_size, args.workers)
    elif args.command == 'index':
        index_corpus(args.epub_dir, args.chunk_size, args.chunk_overlap, args.batch_size)
    elif args.command == 'search':
        search(args.query, args.top_k)


if __name__ == '__main__':
    main()
