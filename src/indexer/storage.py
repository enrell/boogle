import os
from typing import Iterator

from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class IndexStorage:
    def __init__(self, dsn: str | None = None):
        self.dsn = dsn or self._build_dsn()
        self.pool = ConnectionPool(self.dsn, kwargs={"row_factory": dict_row})
        self._init_schema()

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
                CREATE TABLE IF NOT EXISTS idx_documents (
                    doc_id INTEGER PRIMARY KEY,
                    length INTEGER NOT NULL,
                    metadata TEXT
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
            conn.commit()

    def clear(self):
        with self.pool.connection() as conn:
            conn.execute("TRUNCATE idx_documents, idx_terms, idx_globals")
            conn.commit()

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

    def insert_documents_batch(self, docs: list[tuple[int, int, str]]):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO idx_documents (doc_id, length, metadata) VALUES (%s, %s, %s)
                    ON CONFLICT (doc_id) DO UPDATE SET length = EXCLUDED.length, metadata = EXCLUDED.metadata
                """, docs)
            conn.commit()

    def insert_terms_batch(self, terms: list[tuple[str, int, bytes]]):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO idx_terms (term, df, postings) VALUES (%s, %s, %s)
                    ON CONFLICT (term) DO UPDATE SET df = EXCLUDED.df, postings = EXCLUDED.postings
                """, terms)
            conn.commit()

    def get_document(self, doc_id: int) -> tuple[int, str] | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT length, metadata FROM idx_documents WHERE doc_id = %s", (doc_id,)
            ).fetchone()
        return (row["length"], row["metadata"]) if row else None

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

    def iter_documents(self) -> Iterator[tuple[int, int, str]]:
        with self.pool.connection() as conn:
            for row in conn.execute("SELECT doc_id, length, metadata FROM idx_documents ORDER BY doc_id"):
                yield row["doc_id"], row["length"], row["metadata"]

    def iter_terms(self) -> Iterator[tuple[str, int, bytes]]:
        with self.pool.connection() as conn:
            for row in conn.execute("SELECT term, df, postings FROM idx_terms"):
                yield row["term"], row["df"], bytes(row["postings"])

    def close(self):
        self.pool.close()
