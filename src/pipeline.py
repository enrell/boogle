import argparse
import hashlib
from pathlib import Path

from rust_bm25 import analyze, encode_postings, chunk_text, parse_epub, parse_pdf, parse_txt

from src.downloader.downloader import BookSeeder
from src.indexer.storage import IndexStorage
from src.indexer.stopwords import load_stopwords


STOPWORDS = load_stopwords()


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


def file_hash(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def parse_file(path: Path) -> str | None:
    if path.suffix == ".epub":
        return parse_epub(str(path))
    elif path.suffix == ".pdf":
        return parse_pdf(str(path))
    elif path.suffix == ".txt":
        return parse_txt(str(path))
    return None


def index_corpus(
    books_dir: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 100,
    full: bool = False,
):
    books_dir = Path(books_dir)
    book_files = list(books_dir.glob("*.epub")) + list(books_dir.glob("*.txt")) + list(books_dir.glob("*.pdf"))
    total_files = len(book_files)
    
    storage = IndexStorage()
    
    if full:
        storage.clear()
        next_chunk_id = 0
    else:
        next_chunk_id = storage.get_next_chunk_id()
    
    indexed = 0
    skipped = 0
    total_length = 0
    terms_batch: dict[str, list[tuple[int, int]]] = {}
    
    for idx, f in enumerate(book_files):
        book_id = f.stem
        fhash = file_hash(f)
        
        # Skip if already indexed with same hash
        if not full and storage.is_book_indexed(book_id, fhash):
            skipped += 1
            continue
        
        # Parse file
        text = parse_file(f)
        if not text:
            continue
        
        # Chunk text
        chunks = chunk_text(text, chunk_size, chunk_overlap)
        if not chunks:
            continue
        
        # Save chunks to zstd file
        storage.save_book_chunks(book_id, chunks)
        
        # Index chunks
        chunk_records = []
        for local_id, chunk in enumerate(chunks):
            global_id = next_chunk_id + local_id
            tokens = analyze(chunk)
            total_length += len(tokens)
            
            chunk_records.append((global_id, book_id))
            
            term_freqs: dict[str, int] = {}
            for token in tokens:
                if token not in STOPWORDS:
                    term_freqs[token] = term_freqs.get(token, 0) + 1
            
            for term, freq in term_freqs.items():
                if term not in terms_batch:
                    terms_batch[term] = []
                terms_batch[term].append((global_id, freq))
        
        storage.insert_chunks_batch(chunk_records)
        storage.mark_book_indexed(book_id, fhash, len(chunks))
        
        next_chunk_id += len(chunks)
        indexed += 1
        
        if indexed % 100 == 0:
            print(f"Indexed {indexed} books, {next_chunk_id} chunks (skipped {skipped})")
    
    # Save terms
    if terms_batch:
        print(f"Saving {len(terms_batch)} terms...")
        terms_list = []
        for term, postings in terms_batch.items():
            terms_list.append((term, len(postings), encode_postings(postings)))
            if len(terms_list) >= 10000:
                storage.insert_terms_batch(terms_list, merge=not full)
                terms_list = []
        if terms_list:
            storage.insert_terms_batch(terms_list, merge=not full)
    
    # Update globals
    old_num = int(storage.get_global("num_docs") or 0)
    old_total = float(storage.get_global("total_length") or 0)
    
    new_num = next_chunk_id
    new_total = old_total + total_length
    avgdl = new_total / new_num if new_num > 0 else 0
    
    storage.set_global("num_docs", str(new_num))
    storage.set_global("total_length", str(new_total))
    storage.set_global("avgdl", str(avgdl))
    storage.set_global("k1", "1.5")
    storage.set_global("b", "0.75")
    
    print(f"Done: {indexed} indexed, {skipped} skipped, {next_chunk_id} total chunks")


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
        index_corpus(args.books_dir, args.chunk_size, args.chunk_overlap, args.full)
    elif args.command == 'search':
        search(args.query, args.top_k)


if __name__ == '__main__':
    main()
