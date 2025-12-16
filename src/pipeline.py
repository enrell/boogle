import argparse
import hashlib
from pathlib import Path

from rust_bm25 import analyze, encode_postings, chunk_text, parse_epub, parse_pdf, parse_txt, process_batch

from src.downloader.downloader import BookSeeder
from src.indexer.storage import IndexStorage
from src.indexer.stopwords import load_stopwords

STOPWORDS = load_stopwords()


def file_hash(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()

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
    chunks_dir_str = str(storage.chunks_dir)
    
    if full:
        storage.clear()
        next_chunk_id = 0
    else:
        next_chunk_id = storage.get_next_chunk_id()
    
    stopwords_list = list(STOPWORDS)
    
    indexed = 0
    skipped = 0
    total_length = 0
    
    current_paths = []
    current_ids = []
    current_hashes = []
    BATCH_SIZE = 100

    def flush_batch():
        nonlocal next_chunk_id, indexed, skipped, total_length
        if not current_paths:
            return
            
        print(f"Processing batch of {len(current_paths)} books in parallel...")
        
        try:
            (chunk_records, terms_result, batch_len, batch_chunks_count) = process_batch(
                current_paths,
                current_ids,
                chunk_size,
                chunk_overlap,
                next_chunk_id,
                chunks_dir_str,
                stopwords_list
            )
        except Exception as e:
            print(f"Error processing batch: {e}")
            # Fallback or skip?
            return

        if not chunk_records and batch_chunks_count == 0:
            print("Batch produced no chunks.")
            
        # Insert chunks
        if chunk_records:
            storage.insert_chunks_batch(chunk_records)
        
        # Calculate book chunk counts for metadata
        book_counts = {}
        for _, bid in chunk_records:
            book_counts[bid] = book_counts.get(bid, 0) + 1
            
        # Update indexed status
        for bid, h in zip(current_ids, current_hashes):
            cnt = book_counts.get(bid, 0)
            storage.mark_book_indexed(bid, h, cnt)

        # Insert terms
        # Use merge=True always unless full index from scratch and this is the very first batch
        do_merge = not (full and indexed == 0)
        if terms_result:
            storage.insert_terms_batch(terms_result, merge=do_merge)
        
        total_length += batch_len
        next_chunk_id += batch_chunks_count
        indexed += len(current_paths)
        print(f"Indexed {indexed} books, {next_chunk_id} total chunks (skipped {skipped})")

    for idx, f in enumerate(book_files):
        book_id = f.stem
        fhash = file_hash(f)
        
        if not full and storage.is_book_indexed(book_id, fhash):
            skipped += 1
            if skipped % 500 == 0:
                print(f"Skipped {skipped} books...")
            continue
            
        current_paths.append(str(f))
        current_ids.append(book_id)
        current_hashes.append(fhash)
        
        if len(current_paths) >= BATCH_SIZE:
            flush_batch()
            current_paths = []
            current_ids = []
            current_hashes = []
            
    flush_batch()
    
    # Update globals
    old_num = int(storage.get_global("num_docs") or 0)
    old_total = float(storage.get_global("total_length") or 0)
    
    # If full, old values effectively 0/overwritten by current totals (since we started from 0)
    if full:
        new_num = next_chunk_id
        new_total = total_length
    else:
        new_num = next_chunk_id # next_chunk_id started from old max + 1
        # Wait, getting next_chunk_id from DB gave us MAX(id)+1.
        # But global 'num_docs' might count actual documents or chunks?
        # In this system, "document" = "chunk".
        # So new_num should be total count.
        # But if we started with `next_chunk_id`, that is the total count.
        new_num = next_chunk_id
        
        # total_length should be additive
        # But wait, storage.get_global("total_length") retrieves previous total.
        # We should add what we processed.
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
