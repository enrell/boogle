"""
Database schema for Open Library data dump.
"""
import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def init_db(db_path: str = "data/openlibrary.db"):
    """Initialize the Open Library SQLite database."""
    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Enable WAL mode for better concurrency
    cursor.execute("PRAGMA journal_mode=WAL;")
    
    # We only need specific fields for enrichment:
    # - key (OLID)
    # - title (for search)
    # - authors (for search)
    # - ratings data (average, count)
    # - popularity signals (want_to_read, edition_count)
    # - subjects (for future use)
    
    # Main works table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS works (
        key TEXT PRIMARY KEY,
        title TEXT,
        authors TEXT,  -- JSON list of author keys/names
        ratings_average REAL,
        ratings_count INTEGER,
        want_to_read_count INTEGER,
        edition_count INTEGER,
        subjects TEXT, -- JSON list of subjects
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    # FTS5 index for fast search by title/author
    cursor.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS works_fts USING fts5(
        title,
        authors,
        content=works,
        content_rowid=rowid
    );
    """)
    
    # Triggers to keep FTS index in sync
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS works_ai AFTER INSERT ON works BEGIN
      INSERT INTO works_fts(rowid, title, authors) VALUES (new.rowid, new.title, new.authors);
    END;
    """)
    
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS works_ad AFTER DELETE ON works BEGIN
      INSERT INTO works_fts(works_fts, rowid, title, authors) VALUES('delete', old.rowid, old.title, old.authors);
    END;
    """)
    
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS works_au AFTER UPDATE ON works BEGIN
      INSERT INTO works_fts(works_fts, rowid, title, authors) VALUES('delete', old.rowid, old.title, old.authors);
      INSERT INTO works_fts(rowid, title, authors) VALUES (new.rowid, new.title, new.authors);
    END;
    """)
    
    conn.commit()
    conn.close()
    logger.info(f"Initialized Open Library database at {db_path}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
