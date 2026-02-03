import os
import logging
from typing import Dict, List, Optional, Any, Iterator
from contextlib import contextmanager
from sqlalchemy import create_engine, select, text, func, inspect
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.db.models import Base, Book, SeedOffset

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    SQLAlchemy-based database manager replacing the old manual SQL repository.
    Handles connection lifecycle, sessions, and repository methods.
    """
    
    def __init__(self, dsn: Optional[str] = None, use_sqlite: bool = False):
        self.use_sqlite = use_sqlite or os.getenv("USE_SQLITE", "0") == "1"
        self.url = self._get_db_url(dsn)
        
        # Configure engine
        connect_args = {}
        if self.use_sqlite:
            connect_args = {"check_same_thread": False}  # Needed for SQLite with threads
            
        self.engine = create_engine(
            self.url, 
            connect_args=connect_args,
            pool_pre_ping=True,
            # echo=True  # Uncomment for debugging SQL
        )
        
        # Thread-safe session factory
        self.session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(self.session_factory)
        
        # Ensure tables exist (if not using Alembic externally, but strict use suggests Alembic)
        # We assume Alembic has run. If not, auto-create? 
        # User said "implement alembic orm as actual choice", implying Alembic manages schema.
        # But for dev convenience/tests:
        if self.use_sqlite:
             Base.metadata.create_all(self.engine) # Safe for SQLite dev

    def _get_db_url(self, dsn: Optional[str]) -> str:
        if self.use_sqlite:
            db_path = os.getenv("SQLITE_DB_PATH", "data/boogle.db")
            # Ensure dir exists
            os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
            return f"sqlite:///{db_path}"
        
        if dsn:
            return dsn
            
        url = os.getenv("DATABASE_URL")
        if url:
            # Ensure psycopg driver is used
            if url.startswith("postgresql://"):
                url = url.replace("postgresql://", "postgresql+psycopg://", 1)
            return url
            
        user = os.getenv("POSTGRES_USER", "boogle")
        password = os.getenv("POSTGRES_PASSWORD", "boogle")
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "boogle")
        # Use psycopg (v3) driver explicitly
        return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"

    @contextmanager
    def get_session(self) -> Iterator[Session]:
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def close(self):
        self.engine.dispose()

    # --- Repository Methods (Compatibility API) ---

    def upsert_book(self, metadata: Dict) -> None:
        """
        Insert or Update a book record. 
        Match on (source, book_id).
        """
        source = metadata.get("source")
        book_id = str(metadata.get("book_id"))
        if not source or not book_id:
            raise ValueError("source and book_id are required")
            
        # Prepare data dict
        data = {
            "source": source,
            "book_id": book_id,
            "url": metadata.get("url", ""),
            "title": metadata.get("title"),
            "author": metadata.get("author"),
            "illustrator": metadata.get("illustrator"),
            "release_date": metadata.get("release_date"),
            "language": metadata.get("language"),
            "category": metadata.get("category"),
            "original_publication": metadata.get("original_publication"),
            "credits": metadata.get("credits"),
            "copyright_status": metadata.get("copyright_status"),
            "downloads": metadata.get("downloads"),
            # 'files' handled by ORM mapping (list -> JSON)
            "files": metadata.get("files") or [],
        }
        
        # Fallback cover URL for Gutenberg
        cover_url = metadata.get("cover_url")
        if not cover_url and source == "gutenberg":
            cover_url = f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.cover.medium.jpg"
        data["cover_url"] = cover_url

        with self.get_session() as session:
            existing = session.execute(
                select(Book).where(Book.source == source, Book.book_id == book_id)
            ).scalar_one_or_none()
            
            if existing:
                for key, value in data.items():
                    setattr(existing, key, value)
            else:
                new_book = Book(**data)
                session.add(new_book)
            

    def get_book(self, source: str, book_id: str) -> Optional[Dict]:
        """Fetch a book as a dictionary."""
        with self.get_session() as session:
            book = session.execute(
                select(Book).where(Book.source == source, Book.book_id == str(book_id))
            ).scalar_one_or_none()
            
            if book:
                d = book.to_dict()
                return d
            return None

    def search_books(self, query: str, limit: int = 10, source: Optional[str] = None) -> List[Dict]:
        """Search books by title/author substring."""
        term = f"%{query.lower()}%"
        
        stmt = select(Book).where(
            (func.lower(func.coalesce(Book.title, '')).like(term)) | 
            (func.lower(func.coalesce(Book.author, '')).like(term))
        ).order_by(Book.title.asc()).limit(limit)
        
        if source:
            stmt = stmt.where(Book.source == source)
            
        with self.get_session() as session:
            books = session.execute(stmt).scalars().all()
            return [b.to_dict() for b in books]

    def get_seed_offset(self, source: str) -> tuple[int, Optional[str]]:
        with self.get_session() as session:
            offset_rec = session.execute(
                select(SeedOffset).where(SeedOffset.source == source)
            ).scalar_one_or_none()
            
            if offset_rec:
                return offset_rec.position, offset_rec.last_book_id
            return -1, None

    def update_seed_offset(self, source: str, position: int, last_book_id: Optional[str]) -> None:
        with self.get_session() as session:
            offset_rec = session.execute(
                select(SeedOffset).where(SeedOffset.source == source)
            ).scalar_one_or_none()
            
            if offset_rec:
                offset_rec.position = position
                offset_rec.last_book_id = last_book_id
            else:
                new_rec = SeedOffset(
                    source=source,
                    position=position,
                    last_book_id=last_book_id
                )
                session.add(new_rec)

PostgresRepository = DatabaseManager
