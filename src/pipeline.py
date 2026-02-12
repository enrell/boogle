"""
Boogle Search Pipeline

Multi-provider incremental indexing with NRT support.
"""

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import List, Optional

try:
    from rust_bm25 import index_corpus_file, FileSearcher, RealTimeIndexer
except ImportError:
    # Rust module not available (for testing)
    index_corpus_file = None
    FileSearcher = None
    RealTimeIndexer = None

from src.downloader.downloader import BookSeeder
from src.indexer.stopwords import load_stopwords
from src.db.database import PostgresRepository
from src.providers.registry import ProviderRegistry
from src.providers.base import BaseBookProvider

from src.enrichment.openlibrary import OpenLibraryClient
from src.enrichment.service import enrich_books_service


def _index_with_streaming_cleanup(
    books_dir: str,
    index_dir: str,
    chunks_dir: str,
    stopwords: list,
    chunk_size: int,
    chunk_overlap: int,
    batch_size: int,
) -> tuple:
    """
    Index books with streaming cleanup.

    Moves files to temp directory in batches, indexes them, then temp is cleaned.
    This saves disk space by moving (not copying) files.

    Returns:
        Tuple of (indexed_count, total_chunks)
    """
    import shutil
    import tempfile
    from pathlib import Path

    books_path = Path(books_dir)

    # Find all book files
    book_files = []
    for pattern in ["**/*.txt", "**/*.epub", "**/*.pdf"]:
        book_files.extend(books_path.glob(pattern))

    if not book_files:
        print(f"  No book files found in {books_dir}")
        return 0, 0

    print(f"  Found {len(book_files)} files to index")
    print(f"  Processing in batches of {batch_size} to save disk space")

    total_indexed = 0
    total_chunks = 0
    files_processed = 0

    # Process in batches
    for batch_start in range(0, len(book_files), batch_size):
        batch_end = min(batch_start + batch_size, len(book_files))
        batch = book_files[batch_start:batch_end]

        # Create temporary directory for this batch
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # MOVE files to temp directory (saves space vs copying)
            moved_files = []
            for file_path in batch:
                try:
                    temp_file = temp_path / file_path.name
                    shutil.move(str(file_path), str(temp_file))
                    moved_files.append((file_path, temp_file))
                except Exception as e:
                    print(f"  Warning: Could not move {file_path.name}: {e}")
                    continue

            if not moved_files:
                continue

            # Index this batch
            try:
                batch_indexed, batch_chunks = index_corpus_file(
                    str(temp_path),
                    index_dir,
                    chunks_dir,
                    stopwords,
                    chunk_size,
                    chunk_overlap,
                    len(moved_files),
                )

                total_indexed += batch_indexed
                total_chunks += batch_chunks
                files_processed += len(moved_files)

                # Files in temp_dir will be auto-deleted
                # Original files were moved, so they're gone
                for orig_path, _ in moved_files:
                    print(f"  ✓ Indexed and cleaned: {orig_path.name}")

                print(f"  Progress: {files_processed}/{len(book_files)} files")

            except Exception as e:
                print(f"  Error indexing batch: {e}")
                # Try to move files back if indexing failed
                for orig_path, temp_file in moved_files:
                    try:
                        shutil.move(str(temp_file), str(orig_path))
                    except:
                        pass
                continue

    # Clean up empty directories
    for dir_path in sorted(books_path.rglob("*"), reverse=True):
        if dir_path.is_dir() and not any(dir_path.iterdir()):
            try:
                dir_path.rmdir()
            except:
                pass

    return total_indexed, total_chunks


def _cleanup_source_files(books_dir: str, keep_books: bool = False):
    """
    Clean up downloaded source files after indexing.

    When keep_books=False (default), deletes source .txt/.pdf/.epub files
    from books_dir while preserving the directory structure.

    The compressed chunks in data/chunks/ are preserved for search.

    Args:
        books_dir: Directory containing downloaded books
        keep_books: If True, skip cleanup and keep source files
    """
    if keep_books:
        print(f"  Keeping source files (--keep-books)")
        return

    from pathlib import Path
    import glob

    books_path = Path(books_dir)
    if not books_path.exists():
        return

    # Count files before deletion
    deleted_count = 0
    bytes_freed = 0

    # Delete source files (txt, pdf, epub)
    for pattern in ["**/*.txt", "**/*.pdf", "**/*.epub"]:
        for file_path in books_path.glob(pattern):
            try:
                bytes_freed += file_path.stat().st_size
                file_path.unlink()
                deleted_count += 1
            except Exception as e:
                print(f"  Warning: Could not delete {file_path}: {e}")

    # Remove empty directories
    for dir_path in sorted(books_path.rglob("*"), reverse=True):
        if dir_path.is_dir() and not any(dir_path.iterdir()):
            try:
                dir_path.rmdir()
            except:
                pass

    mb_freed = bytes_freed / (1024 * 1024)
    print(f"  Deleted {deleted_count} source files, freed {mb_freed:.1f} MB")
    print(f"  Compressed chunks preserved in data/chunks/")


def get_providers(provider_names: Optional[List[str]] = None) -> List[BaseBookProvider]:
    """
    Get list of providers to index.

    Args:
        provider_names: Specific provider names, or None for all enabled

    Returns:
        List of provider instances
    """
    # Auto-discover providers
    ProviderRegistry.auto_discover()

    if provider_names:
        # Get specific providers
        providers = []
        for name in provider_names:
            try:
                provider = ProviderRegistry.get(name)
                providers.append(provider)
            except ValueError as e:
                print(f"Warning: {e}")
        return providers
    else:
        # Get all enabled providers
        return ProviderRegistry.get_enabled()


def run_index_pipeline(
    limit: int | None = None,
    batch_size: int = 1000,
    use_sqlite: bool = False,
    reindex: bool = False,
    chunk_size: int = 1000,
    chunk_overlap: int = 100,
    workers: int = 16,
    enrich: bool = False,
    light_mode: bool = False,
    providers: Optional[List[str]] = None,
    use_nrt: bool = False,
    cross_reference: bool = True,
    keep_books: bool = False,
):
    """
    Run the complete indexing pipeline.

    Features:
    - Multi-provider seeding (all enabled by default)
    - Incremental updates (resumes from last position)
    - Cross-reference merging (detects duplicates across providers)
    - NRT indexing (adds new books without rebuilding)
    - Full reindex option (--reindex)

    Args:
        limit: Maximum books per provider (None = unlimited)
        batch_size: Batch size for checkpointing
        use_sqlite: Use SQLite instead of PostgreSQL
        reindex: Force full rebuild (clear existing index)
        chunk_size: Text chunk size for indexing
        chunk_overlap: Text chunk overlap
        workers: Number of parallel workers
        enrich: Enrich metadata from OpenLibrary
        light_mode: Metadata-only (skip downloads)
        providers: Specific providers to index (None = all enabled)
        use_nrt: Use RealTimeIndexer for incremental updates
        cross_reference: Cross-reference after each provider
        keep_books: Keep downloaded source files after indexing (default: False)
    """
    books_dir = os.getenv("BOOKS_DIR", "data/books")
    index_dir = os.getenv("INDEX_DIR", "data/index")
    chunks_dir = os.getenv("CHUNKS_DIR", "data/chunks")
    metadata_index_dir = os.getenv("METADATA_INDEX_DIR", "data/index_metadata")
    metadata_chunks_dir = os.getenv("METADATA_CHUNKS_DIR", "data/chunks_metadata")

    # Step 1: Get providers
    print("\n" + "=" * 60)
    print("Step 1: Discovering Providers")
    print("=" * 60)

    provider_list = get_providers(providers)

    if not provider_list:
        print("Error: No providers available. Check ProviderRegistry.")
        return 0

    print(f"Found {len(provider_list)} provider(s):")
    for p in provider_list:
        enabled = "enabled" if p.enabled_by_default else "disabled by default"
        print(f"  - {p.source_name} ({enabled})")

    # Step 2: Reset checkpoints and clear books if reindexing
    if reindex:
        print("\n" + "=" * 60)
        print("Resetting provider checkpoints for full reindex")
        print("=" * 60)
        db = PostgresRepository(use_sqlite=use_sqlite)
        db.reset_seed_offsets()
        deleted = db.clear_all_books()
        db.close()
        print(f"Checkpoints reset - will re-download from beginning")
        print(f"Cleared {deleted} existing books from database")

    # Step 3: Seed corpus
    print("\n" + "=" * 60)
    print(f"Step 3: Seeding Corpus")
    print(f" SQLite: {use_sqlite}")
    print(f" Light Mode: {light_mode}")
    print(f" Cross-reference: {cross_reference}")
    print("=" * 60)

    seeder = BookSeeder(
        providers=provider_list,
        output_dir=books_dir,
        max_workers=workers,
        use_sqlite=use_sqlite,
        light_mode=light_mode,
    )

    try:
        seed_results = seeder.seed_all(
            limit=limit,
            batch_size=batch_size,
            cross_reference=cross_reference,
        )

        total_new = sum(seed_results.values())
        print(f"\nSeeding complete: {total_new} new books total")
        for name, count in seed_results.items():
            print(f"  - {name}: {count} books")
    finally:
        seeder.close()

    if total_new == 0 and not reindex:
        print("\nNo new books found. Index is up to date.")
        return 0

    # Step 4: Optional enrichment
    if enrich:
        print("\n" + "=" * 60)
        print("Step 4: Enriching Metadata")
        print("=" * 60)

        try:
            db_manager = PostgresRepository(use_sqlite=use_sqlite)
            ol_client = OpenLibraryClient()
            enrich_books_service(db_manager, ol_client, limit=limit)
            db_manager.close()
        except Exception as e:
            print(f"Enrichment failed: {e}")
            print("Continuing with indexing...")

    # Step 5: Indexing
    if light_mode:
        # Light mode: metadata-only index
        print("\n" + "=" * 60)
        print("Step 5: Building Metadata Index (Light Mode)")
        print("=" * 60)

        from src.indexer.metadata_indexer import index_metadata

        Path(metadata_index_dir).mkdir(parents=True, exist_ok=True)
        Path(metadata_chunks_dir).mkdir(parents=True, exist_ok=True)

        if reindex:
            print(f"Clearing existing metadata index...")
            if Path(metadata_index_dir).exists():
                shutil.rmtree(metadata_index_dir)
            Path(metadata_index_dir).mkdir(parents=True, exist_ok=True)

        indexed = index_metadata(
            index_dir=metadata_index_dir,
            chunks_dir=metadata_chunks_dir,
            use_sqlite=use_sqlite,
            limit=limit,
        )

        print(f"Metadata indexing complete: {indexed} books")
        return indexed

    # Full-text indexing
    print("\n" + "=" * 60)
    print(f"Step 5: Building Index")
    print(f"  Mode: {'NRT (incremental)' if use_nrt else 'Batch (full rebuild)'}")
    print(f"  Reindex: {reindex}")
    print("=" * 60)

    Path(index_dir).mkdir(parents=True, exist_ok=True)
    Path(chunks_dir).mkdir(parents=True, exist_ok=True)

    if use_nrt and not reindex:
        # NRT mode: Add only new books
        print(f"Using RealTimeIndexer for incremental updates...")

        # Get new books from database
        db = PostgresRepository(use_sqlite=use_sqlite)
        new_books = []

        for source in seed_results.keys():
            if seed_results[source] > 0:
                # Get books from this source
                books = db.get_books_by_source(source, limit=seed_results[source])
                new_books.extend(books)

        db.close()

        if not new_books:
            print("No new books to index")
            return 0

        # Open RealTimeIndexer
        indexer = RealTimeIndexer(index_dir)
        stopwords = list(load_stopwords())
        indexer.set_stopwords(stopwords)

        # Process each new book
        indexed_count = 0
        for book in new_books:
            book_id = book.get("book_id")
            local_path = book.get("local_path")

            if not local_path or not Path(local_path).exists():
                continue

            # Read book content
            try:
                content = Path(local_path).read_text(encoding="utf-8", errors="ignore")
            except Exception as e:
                print(f"Error reading {local_path}: {e}")
                continue

            # Chunk content
            chunks = []
            start = 0
            while start < len(content):
                end = start + chunk_size
                chunk = content[start:end]
                chunks.append(chunk)
                start = end - chunk_overlap

            # Add chunks to index
            for chunk_id, chunk_text in enumerate(chunks):
                metadata = json.dumps(
                    {
                        "book_id": book_id,
                        "chunk_id": chunk_id,
                        "title": book.get("title", "Unknown"),
                        "author": book.get("author", "Unknown"),
                        "source": book.get("source", "unknown"),
                    }
                )

                try:
                    indexer.add_document(chunk_text, metadata)
                    indexed_count += 1
                except Exception as e:
                    print(f"Error adding chunk {chunk_id} of {book_id}: {e}")

            if indexed_count % 100 == 0:
                print(f"Indexed {indexed_count} chunks...")

            # Delete source file immediately after indexing (dynamic cleanup)
            if not keep_books and local_path and Path(local_path).exists():
                try:
                    Path(local_path).unlink()
                    print(f"  Cleaned up: {Path(local_path).name}")
                except Exception as e:
                    print(f"  Warning: Could not delete {local_path}: {e}")

        print(
            f"NRT indexing complete: {indexed_count} chunks from {len(new_books)} books"
        )
        return len(new_books)

    else:
        # Batch mode: Full reindex
        if reindex:
            print(f"Clearing existing index at {index_dir}...")
            if Path(index_dir).exists():
                shutil.rmtree(index_dir)
            Path(index_dir).mkdir(parents=True, exist_ok=True)

        stopwords = list(load_stopwords())

        # Stream files and delete after indexing to save disk space
        if not keep_books:
            print(
                f"Indexing with streaming cleanup (source files deleted after indexing)..."
            )
            indexed, total_chunks = _index_with_streaming_cleanup(
                books_dir,
                index_dir,
                chunks_dir,
                stopwords,
                chunk_size,
                chunk_overlap,
                batch_size,
            )
        else:
            print(f"Indexing files from {books_dir}...")
            indexed, total_chunks = index_corpus_file(
                books_dir,
                index_dir,
                chunks_dir,
                stopwords,
                chunk_size,
                chunk_overlap,
                batch_size,
            )

        print(f"Batch indexing complete: {indexed} books, {total_chunks} chunks")

        # Clean up source files if not keeping books
        if not keep_books and not light_mode:
            print(f"\nCleaning up source files (keep_books=False)...")
            _cleanup_source_files(books_dir, keep_books)
        elif keep_books:
            print(f"  Keeping source files (--keep-books)")

        return indexed


def search(
    query: str,
    top_k: int = 10,
    use_sqlite: bool = False,
    light_mode: bool = False,
):
    """Search the index."""
    if light_mode:
        # Light mode search
        from src.indexer.metadata_indexer import MetadataIndexer

        metadata_index_dir = os.getenv("METADATA_INDEX_DIR", "data/index_metadata")
        metadata_path = Path(metadata_index_dir)

        # Check if metadata index exists
        if not (metadata_path / "index.json").exists():
            print(f"Error: Metadata index not found at {metadata_index_dir}")
            print(
                "Please run 'boogle index --light-mode' first to build the metadata index."
            )
            return

        print(f"Searching metadata index...")
        indexer = MetadataIndexer(index_dir=metadata_index_dir, use_sqlite=use_sqlite)

        try:
            results = indexer.search(query, top_k * 10)
        finally:
            indexer.close()

        count = 0
        for book_id, score in results:
            # Try to get from any source
            meta = None
            for source in ["gutenberg", "openlibrary", "pportal"]:
                meta = indexer.db.get_book(source, book_id)
                if meta:
                    break

            if meta:
                title = meta.get("title", "Unknown")
                author = meta.get("author", "Unknown")
                print(f"[{score:.4f}] {title} by {author} (book={book_id})")
                count += 1
                if count >= top_k:
                    break
    else:
        # Full-text search
        index_dir = os.getenv("INDEX_DIR", "data/index")
        index_path = Path(index_dir)

        # Check if index exists
        if not (index_path / "index.json").exists():
            print(f"Error: Index not found at {index_dir}")
            print("Please run 'boogle index' first to build the index.")
            return

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

            # Try to get from any source
            meta = None
            for source in ["gutenberg", "openlibrary", "pportal"]:
                meta = db.get_book(source, book_id)
                if meta:
                    break

            if meta:
                title = meta.get("title", "Unknown")
                author = meta.get("author", "Unknown")
                rating = meta.get("ratings_average")
                rating_str = f" [Rating: {rating:.1f}]" if rating else ""
                print(f"[{score:.4f}] {title} by {author}{rating_str} (book={book_id})")
                count += 1
                if count >= top_k:
                    break

        db.close()


def run_api(
    host: str = "0.0.0.0",
    port: int = 8000,
    use_sqlite: bool = False,
    light_mode: bool = False,
    use_nrt: bool = False,
):
    """Run the REST API."""
    import uvicorn

    if use_sqlite:
        os.environ["USE_SQLITE"] = "1"
        print("Starting API in SQLite mode (data/boogle.db)")
    else:
        print("Starting API in PostgreSQL mode")

    if light_mode:
        os.environ["LIGHT_MODE"] = "1"
        print("Using metadata-only (light) mode")

    if use_nrt:
        os.environ["REALTIME_INDEX"] = "1"
        print("Using RealTimeIndexer")

    uvicorn.run("src.api.main:app", host=host, port=port, reload=True)


def main():
    parser = argparse.ArgumentParser(description="Boogle Search Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Index command
    idx_parser = subparsers.add_parser("index", help="Seed corpus and build index")
    idx_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit books per provider (default: unlimited)",
    )
    idx_parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for checkpointing"
    )
    idx_parser.add_argument(
        "--sqlite", action="store_true", help="Use SQLite instead of PostgreSQL"
    )
    idx_parser.add_argument(
        "--reindex",
        action="store_true",
        help="Force full rebuild (clear existing index)",
    )
    idx_parser.add_argument(
        "--workers", type=int, default=16, help="Parallel workers for downloads"
    )
    idx_parser.add_argument(
        "--chunk-size", type=int, default=1000, help="Text chunk size"
    )
    idx_parser.add_argument(
        "--chunk-overlap", type=int, default=100, help="Text chunk overlap"
    )
    idx_parser.add_argument(
        "--enrich", action="store_true", help="Enrich metadata from Open Library"
    )
    idx_parser.add_argument(
        "--light-mode",
        action="store_true",
        help="Metadata-only (skip full text downloads)",
    )
    idx_parser.add_argument(
        "--providers",
        nargs="+",
        default=None,
        help="Specific providers to index (default: all enabled)",
    )
    idx_parser.add_argument(
        "--nrt",
        action="store_true",
        help="Use RealTimeIndexer for incremental updates",
    )
    idx_parser.add_argument(
        "--no-cross-reference",
        action="store_true",
        help="Skip cross-reference merging",
    )
    idx_parser.add_argument(
        "--keep-books",
        action="store_true",
        help="Keep downloaded source files after indexing (default: delete to save space)",
    )

    # Search command
    search_parser = subparsers.add_parser("search", help="Search the index")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument(
        "--top-k", type=int, default=10, help="Number of results"
    )
    search_parser.add_argument("--sqlite", action="store_true", help="Use SQLite")
    search_parser.add_argument(
        "--light-mode", action="store_true", help="Search metadata-only index"
    )

    # API command
    api_parser = subparsers.add_parser("api", help="Run the REST API")
    api_parser.add_argument("--host", default="0.0.0.0")
    api_parser.add_argument("--port", type=int, default=8000)
    api_parser.add_argument("--sqlite", action="store_true", help="Use SQLite")
    api_parser.add_argument(
        "--light-mode",
        action="store_true",
        help="Use metadata-only index (no full text)",
    )
    api_parser.add_argument(
        "--nrt",
        action="store_true",
        help="Use RealTimeIndexer",
    )

    args = parser.parse_args()

    if args.command == "index":
        run_index_pipeline(
            limit=args.limit,
            batch_size=args.batch_size,
            use_sqlite=args.sqlite,
            reindex=args.reindex,
            chunk_size=args.chunk_size,
            chunk_overlap=args.chunk_overlap,
            workers=args.workers,
            enrich=args.enrich,
            light_mode=args.light_mode,
            providers=args.providers,
            use_nrt=args.nrt,
            cross_reference=not args.no_cross_reference,
            keep_books=args.keep_books,
        )
    elif args.command == "search":
        search(args.query, args.top_k, args.sqlite, args.light_mode)
    elif args.command == "api":
        run_api(args.host, args.port, args.sqlite, args.light_mode, args.nrt)


if __name__ == "__main__":
    main()
