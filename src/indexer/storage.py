import os
from functools import lru_cache
from pathlib import Path

import zstandard as zstd
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from rust_bm25 import merge_postings

CHUNKS_DIR = Path(os.getenv("CHUNKS_DIR", "data/chunks"))
CACHE_MAX_BOOKS = int(os.getenv("CACHE_MAX_BOOKS", "500"))  # ~100MB for avg 200KB/book


class IndexStorage:
    def __init__(self, dsn: str | None = None):
        self.dsn = dsn or self._build_dsn()
        self.pool = ConnectionPool(self.dsn, min_size=1, max_size=10, kwargs={"row_factory": dict_row})
        self.chunks_dir = CHUNKS_DIR
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        self._cctx = zstd.ZstdCompressor(level=9)
        self._dctx = zstd.ZstdDecompressor()
        self._init_schema()
        
        # LRU cache for decompressed book chunks
        self._get_book_chunks = lru_cache(maxsize=CACHE_MAX_BOOKS)(self._load_book_chunks)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self.pool.close(timeout=1)

    def _build_dsn(self) -> str:
        url = os.getenv("DATABASE_URL")
        if url:
            return url
        user = os.getenv("POSTGRES_USER", "boogle")
        password = os.getenv("POSTGRES_PASSWORD", "boogle")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "boogle")
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def _init_schema(self):
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS idx_chunks (
                    chunk_id INTEGER PRIMARY KEY,
                    book_id TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS idx_terms (
                    term TEXT PRIMARY KEY,
                    df INTEGER NOT NULL,
                    postings BYTEA NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS idx_globals (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS idx_books_indexed (
                    book_id TEXT PRIMARY KEY,
                    file_hash TEXT NOT NULL,
                    chunk_count INTEGER NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_book ON idx_chunks(book_id)")
            conn.commit()

    def is_book_indexed(self, book_id: str, file_hash: str) -> bool:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT file_hash FROM idx_books_indexed WHERE book_id = %s", (book_id,)
            ).fetchone()
        return row is not None and row["file_hash"] == file_hash

    def get_indexed_books_batch(self, book_ids: list[str]) -> dict[str, str]:
        """Get {book_id: file_hash} for multiple books."""
        if not book_ids:
            return {}
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT book_id, file_hash FROM idx_books_indexed WHERE book_id = ANY(%s)", (book_ids,)
            ).fetchall()
        return {row["book_id"]: row["file_hash"] for row in rows}

    def mark_book_indexed(self, book_id: str, file_hash: str, chunk_count: int):
        with self.pool.connection() as conn:
            conn.execute("""
                INSERT INTO idx_books_indexed (book_id, file_hash, chunk_count) VALUES (%s, %s, %s)
                ON CONFLICT (book_id) DO UPDATE SET file_hash = EXCLUDED.file_hash, chunk_count = EXCLUDED.chunk_count
            """, (book_id, file_hash, chunk_count))
            conn.commit()

    def mark_books_indexed_batch(self, books: list[tuple[str, str, int]]):
        """Mark multiple books as indexed. books = [(book_id, file_hash, chunk_count), ...]"""
        if not books:
            return
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO idx_books_indexed (book_id, file_hash, chunk_count) VALUES (%s, %s, %s)
                    ON CONFLICT (book_id) DO UPDATE SET file_hash = EXCLUDED.file_hash, chunk_count = EXCLUDED.chunk_count
                """, books)
            conn.commit()

    def get_next_chunk_id(self) -> int:
        with self.pool.connection() as conn:
            row = conn.execute("SELECT COALESCE(MAX(chunk_id), -1) + 1 as next_id FROM idx_chunks").fetchone()
        return row["next_id"]

    def _get_chunk_path(self, book_id: str) -> Path:
        """Get path with sharded directory structure."""
        # Use first 2 chars of book_id as shard
        shard = book_id[:2].zfill(2)
        shard_dir = self.chunks_dir / shard
        shard_dir.mkdir(exist_ok=True)
        return shard_dir / f"{book_id}.zst"

    def clear(self):
        with self.pool.connection() as conn:
            conn.execute("TRUNCATE idx_chunks, idx_terms, idx_globals")
            conn.commit()
        # Clear chunk files
        import shutil
        for d in self.chunks_dir.iterdir():
            if d.is_dir():
                shutil.rmtree(d)
            elif d.suffix == ".zst":
                d.unlink()

    def set_global(self, key: str, value: str):
        with self.pool.connection() as conn:
            conn.execute("""
                INSERT INTO idx_globals (key, value) VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (key, value))
            conn.commit()

    def get_global(self, key: str) -> str | None:
        with self.pool.connection() as conn:
            row = conn.execute("SELECT value FROM idx_globals WHERE key = %s", (key,)).fetchone()
        return row["value"] if row else None

    def save_book_chunks(self, book_id: str, chunks: list[str]):
        """Save chunks to zstd file."""
        if not chunks:
            return
        text = "\n".join(chunks)
        compressed = self._cctx.compress(text.encode("utf-8"))
        self._get_chunk_path(book_id).write_bytes(compressed)
        # Invalidate cache for this book
        self._get_book_chunks.cache_clear()

    def _load_book_chunks(self, book_id: str) -> list[str] | None:
        """Load and decompress all chunks for a book (cached)."""
        chunk_file = self._get_chunk_path(book_id)
        if not chunk_file.exists():
            return None
        compressed = chunk_file.read_bytes()
        text = self._dctx.decompress(compressed).decode("utf-8")
        return text.split("\n")

    def get_chunk_text(self, book_id: str, local_chunk_id: int) -> str | None:
        """Get chunk text from cache or zstd file."""
        chunks = self._get_book_chunks(book_id)
        if chunks and local_chunk_id < len(chunks):
            return chunks[local_chunk_id]
        return None
    
    def cache_stats(self) -> dict:
        """Return cache statistics."""
        info = self._get_book_chunks.cache_info()
        return {"hits": info.hits, "misses": info.misses, "size": info.currsize, "maxsize": info.maxsize}

    def insert_chunks_batch(self, chunks: list[tuple[int, str]]):
        """Insert (chunk_id, book_id) tuples."""
        with self.pool.connection() as conn:
            with conn.cursor().copy("COPY idx_chunks (chunk_id, book_id) FROM STDIN") as copy:
                for chunk in chunks:
                    copy.write_row(chunk)
            conn.commit()

    def insert_terms_batch(self, terms: list[tuple[str, int, bytes]], merge: bool = False):
        if not merge:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO idx_terms (term, df, postings) VALUES (%s, %s, %s)
                        ON CONFLICT (term) DO NOTHING
                    """, terms)
                conn.commit()
            return
        
        term_names = [t[0] for t in terms]
        existing = self.get_terms_batch(term_names)
        
        merged = []
        for term, df, postings in terms:
            if term in existing:
                old_df, old_postings = existing[term]
                new_postings = merge_postings(old_postings, postings)
                merged.append((term, old_df + df, new_postings))
            else:
                merged.append((term, df, postings))
        
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO idx_terms (term, df, postings) VALUES (%s, %s, %s)
                    ON CONFLICT (term) DO UPDATE SET df = EXCLUDED.df, postings = EXCLUDED.postings
                """, merged)
            conn.commit()

    def get_chunks_batch(self, chunk_ids: list[int]) -> dict[int, str]:
        """Get {chunk_id: book_id} for multiple chunks."""
        if not chunk_ids:
            return {}
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT chunk_id, book_id FROM idx_chunks WHERE chunk_id = ANY(%s)", (chunk_ids,)
            ).fetchall()
        return {row["chunk_id"]: row["book_id"] for row in rows}

    def get_term(self, term: str) -> tuple[int, bytes] | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT df, postings FROM idx_terms WHERE term = %s", (term,)
            ).fetchone()
        return (row["df"], bytes(row["postings"])) if row else None

    def get_terms_batch(self, terms: list[str]) -> dict[str, tuple[int, bytes]]:
        if not terms:
            return {}
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT term, df, postings FROM idx_terms WHERE term = ANY(%s)", (terms,)
            ).fetchall()
        return {row["term"]: (row["df"], bytes(row["postings"])) for row in rows}

    def get_books_metadata(self, book_ids: list[str]) -> dict[str, dict]:
        """Get book metadata from books table."""
        if not book_ids:
            return {}
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT book_id, title, author FROM books WHERE book_id = ANY(%s)", (book_ids,)
            ).fetchall()
        
        from rust_bm25 import analyze
        result = {}
        for row in rows:
            title = row.get("title") or ""
            author = row.get("author") or ""
            result[row["book_id"]] = {
                "title": title,
                "author": author,
                "title_tokens": analyze(f"{title} {author}"),
            }
        return result
