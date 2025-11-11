import json
import os
import sqlite3
from threading import Lock
from typing import Dict, List, Optional


class SQLiteRepository:
    """Tiny helper around sqlite3 for caching Gutenberg metadata locally."""

    def __init__(self, db_path: str = "data/boogle.db"):
        self.db_path = db_path
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = Lock()
        self._init_db()

    def _init_db(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS books (
                    book_id INTEGER PRIMARY KEY,
                    url TEXT NOT NULL,
                    title TEXT,
                    author TEXT,
                    illustrator TEXT,
                    release_date TEXT,
                    language TEXT,
                    category TEXT,
                    original_publication TEXT,
                    credits TEXT,
                    copyright_status TEXT,
                    downloads TEXT,
                    files TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

    def upsert_book(self, metadata: Dict) -> None:
        files = metadata.get("files") or []
        payload = {
            **metadata,
            "files": json.dumps(files),
        }
        columns = [
            "book_id",
            "url",
            "title",
            "author",
            "illustrator",
            "release_date",
            "language",
            "category",
            "original_publication",
            "credits",
            "copyright_status",
            "downloads",
            "files",
        ]
        values = [payload.get(column) for column in columns]
        with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO books (
                        book_id, url, title, author, illustrator, release_date,
                        language, category, original_publication, credits,
                        copyright_status, downloads, files
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(book_id) DO UPDATE SET
                        url=excluded.url,
                        title=excluded.title,
                        author=excluded.author,
                        illustrator=excluded.illustrator,
                        release_date=excluded.release_date,
                        language=excluded.language,
                        category=excluded.category,
                        original_publication=excluded.original_publication,
                        credits=excluded.credits,
                        copyright_status=excluded.copyright_status,
                        downloads=excluded.downloads,
                        files=excluded.files,
                        updated_at=CURRENT_TIMESTAMP;
                    """,
                    values,
                )

    def get_book(self, book_id: int) -> Optional[Dict]:
        cursor = self._conn.execute(
            """
            SELECT
                book_id, url, title, author, illustrator, release_date,
                language, category, original_publication, credits,
                copyright_status, downloads, files
            FROM books
            WHERE book_id = ?;
            """,
            (book_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        data = dict(row)
        files = data.get("files")
        data["files"] = json.loads(files) if files else []
        return data

    def search_books(self, query: str, limit: int = 10) -> List[Dict]:
        like_query = f"%{query.lower()}%"
        cursor = self._conn.execute(
            """
            SELECT book_id, title, url
            FROM books
            WHERE lower(coalesce(title, '')) LIKE ?
               OR lower(coalesce(author, '')) LIKE ?
            ORDER BY title ASC
            LIMIT ?;
            """,
            (like_query, like_query, limit),
        )
        return [dict(row) for row in cursor.fetchall()]

    def close(self) -> None:
        self._conn.close()
