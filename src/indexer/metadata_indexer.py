"""
Metadata-only indexer for Light Mode.

Indexes book metadata (title, author, subjects, language) without downloading full text.
This enables fast indexing of large catalogs with minimal storage.
"""

import os
import json
import threading
from pathlib import Path
from typing import Iterator, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import zstandard as zstd
from src.db.database import PostgresRepository
from src.indexer.stopwords import load_stopwords


class MetadataIndexer:
    """
    Indexes book metadata for Light Mode.

    Creates a lightweight inverted index on title, author, and subjects
    without requiring full text downloads.
    """

    def __init__(
        self,
        index_dir: str = "data/index_metadata",
        chunks_dir: str = "data/chunks_metadata",
        use_sqlite: bool = False,
        max_workers: int = 16,
    ):
        self.index_dir = Path(index_dir)
        self.chunks_dir = Path(chunks_dir)
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.chunks_dir.mkdir(parents=True, exist_ok=True)

        self.db = PostgresRepository(use_sqlite=use_sqlite)
        self.max_workers = max_workers
        self.stopwords = set(load_stopwords())

        # In-memory index structures
        self._term_index: Dict[
            str, List[Tuple[str, int]]
        ] = {}  # term -> [(book_id, freq)]
        self._doc_lengths: Dict[str, int] = {}  # book_id -> total terms
        self._total_docs = 0
        self._total_doc_length = 0
        self._avg_doc_length = 0.0

    def _tokenize(self, text: str) -> List[str]:
        """Simple tokenization for metadata fields."""
        if not text:
            return []

        # Lowercase and extract alphanumeric tokens
        tokens = []
        current_token = []

        for char in text.lower():
            if char.isalnum():
                current_token.append(char)
            elif current_token:
                token = "".join(current_token)
                if token not in self.stopwords:
                    tokens.append(token)
                current_token = []

        # Don't forget last token
        if current_token:
            token = "".join(current_token)
            if token not in self.stopwords:
                tokens.append(token)

        return tokens

    def _extract_metadata_text(self, book: Dict) -> str:
        """Extract searchable text from book metadata."""
        parts = []

        # Title (highest weight - add multiple times)
        if book.get("title"):
            parts.extend([book["title"]] * 3)

        # Author
        if book.get("author"):
            parts.append(book["author"])

        # Category/Subjects (high weight for discovery)
        if book.get("category"):
            parts.extend([book["category"]] * 2)

        # Language
        if book.get("language"):
            parts.append(book["language"])

        # Illustrator
        if book.get("illustrator"):
            parts.append(book["illustrator"])

        # Original publication info
        if book.get("original_publication"):
            parts.append(book["original_publication"])

        return " ".join(parts)

    def _index_book(self, book: Dict) -> Optional[Tuple[str, int]]:
        """Index a single book's metadata. Returns (book_id, term_count) or None."""
        book_id = book.get("book_id")
        if not book_id:
            return None

        # Extract and tokenize metadata
        metadata_text = self._extract_metadata_text(book)
        tokens = self._tokenize(metadata_text)

        if not tokens:
            return None

        # Count term frequencies
        term_freqs = {}
        for token in tokens:
            term_freqs[token] = term_freqs.get(token, 0) + 1

        # Add to index
        for term, freq in term_freqs.items():
            if term not in self._term_index:
                self._term_index[term] = []
            self._term_index[term].append((book_id, freq))

        doc_length = len(tokens)
        self._doc_lengths[book_id] = doc_length

        return book_id, doc_length

    def index_books(self, limit: Optional[int] = None) -> int:
        """
        Index all books in the database.

        Returns number of books indexed.
        """
        print(f"Starting metadata indexing...")

        # Fetch all books
        from sqlalchemy import select
        from src.db.models import Book

        books_indexed = 0
        batch_size = 1000
        offset = 0

        while True:
            with self.db.get_session() as session:
                stmt = select(Book).limit(batch_size).offset(offset)
                books = session.execute(stmt).scalars().all()

                if not books:
                    break

                for book in books:
                    result = self._index_book(book.to_dict())
                    if result:
                        books_indexed += 1

                offset += batch_size

                if limit and books_indexed >= limit:
                    break

                if books_indexed % 10000 == 0:
                    print(f"Indexed {books_indexed} books...")

        # Calculate statistics
        self._total_docs = len(self._doc_lengths)
        self._total_doc_length = sum(self._doc_lengths.values())
        self._avg_doc_length = (
            self._total_doc_length / self._total_docs if self._total_docs > 0 else 0
        )

        # Persist index to disk
        self._save_index()

        print(f"Metadata indexing complete: {books_indexed} books indexed")
        print(f"Unique terms: {len(self._term_index)}")
        print(f"Avg doc length: {self._avg_doc_length:.2f}")

        return books_indexed

    def _save_index(self):
        """Save the metadata index to disk."""
        # Save term index
        term_index_path = self.index_dir / "term_index.json.zst"
        with zstd.open(term_index_path, "wt", encoding="utf-8") as f:
            json.dump(self._term_index, f)

        # Save document lengths
        doc_lengths_path = self.index_dir / "doc_lengths.json.zst"
        with zstd.open(doc_lengths_path, "wt", encoding="utf-8") as f:
            json.dump(self._doc_lengths, f)

        # Save metadata
        meta_path = self.index_dir / "meta.json"
        with open(meta_path, "w") as f:
            json.dump(
                {
                    "total_docs": self._total_docs,
                    "total_doc_length": self._total_doc_length,
                    "avg_doc_length": self._avg_doc_length,
                    "index_type": "metadata",
                },
                f,
            )

    def load_index(self):
        """Load the metadata index from disk."""
        term_index_path = self.index_dir / "term_index.json.zst"
        doc_lengths_path = self.index_dir / "doc_lengths.json.zst"
        meta_path = self.index_dir / "meta.json"

        if not term_index_path.exists():
            raise FileNotFoundError(f"Index not found at {term_index_path}")

        # Load term index
        with zstd.open(term_index_path, "rt", encoding="utf-8") as f:
            self._term_index = json.load(f)

        # Load document lengths
        with zstd.open(doc_lengths_path, "rt", encoding="utf-8") as f:
            self._doc_lengths = json.load(f)

        # Load metadata
        with open(meta_path, "r") as f:
            meta = json.load(f)
            self._total_docs = meta["total_docs"]
            self._total_doc_length = meta["total_doc_length"]
            self._avg_doc_length = meta["avg_doc_length"]

    def search(
        self, query: str, top_k: int = 10, k1: float = 1.5, b: float = 0.75
    ) -> List[Tuple[str, float]]:
        """
        Search the metadata index using BM25 scoring.

        Returns list of (book_id, score) tuples, sorted by score descending.
        """
        if not self._term_index:
            self.load_index()

        # Tokenize query
        query_tokens = self._tokenize(query)
        if not query_tokens:
            return []

        # Calculate scores for each document
        doc_scores: Dict[str, float] = {}

        for term in query_tokens:
            if term not in self._term_index:
                continue

            # Get postings for this term
            postings = self._term_index[term]
            df = len(postings)  # Document frequency

            # IDF calculation
            idf = self._idf(df)

            for book_id, tf in postings:
                # BM25 scoring
                doc_length = self._doc_lengths.get(book_id, 0)

                # Term frequency normalization
                tf_norm = tf / (
                    tf + k1 * (1 - b + b * doc_length / self._avg_doc_length)
                )

                score = idf * tf_norm

                if book_id in doc_scores:
                    doc_scores[book_id] += score
                else:
                    doc_scores[book_id] = score

        # Sort by score and return top_k
        sorted_results = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)[
            :top_k
        ]

        return sorted_results

    def _idf(self, df: int) -> float:
        """Calculate IDF for a term."""
        if df == 0:
            return 0
        import math

        return math.log((self._total_docs - df + 0.5) / (df + 0.5) + 1)

    def close(self):
        """Close database connection."""
        self.db.close()


# Convenience function for CLI usage
def index_metadata(
    index_dir: str = "data/index_metadata",
    chunks_dir: str = "data/chunks_metadata",
    use_sqlite: bool = False,
    limit: Optional[int] = None,
) -> int:
    """
    Create a metadata-only index for Light Mode.

    Args:
        index_dir: Directory to save the index
        chunks_dir: Directory for chunk storage (minimal in light mode)
        use_sqlite: Use SQLite instead of PostgreSQL
        limit: Limit number of books to index

    Returns:
        Number of books indexed
    """
    indexer = MetadataIndexer(
        index_dir=index_dir, chunks_dir=chunks_dir, use_sqlite=use_sqlite
    )

    try:
        count = indexer.index_books(limit=limit)
        return count
    finally:
        indexer.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Build metadata-only index (Light Mode)"
    )
    parser.add_argument(
        "--index-dir", default="data/index_metadata", help="Index directory"
    )
    parser.add_argument(
        "--chunks-dir", default="data/chunks_metadata", help="Chunks directory"
    )
    parser.add_argument("--sqlite", action="store_true", help="Use SQLite")
    parser.add_argument("--limit", type=int, default=None, help="Limit books to index")

    args = parser.parse_args()

    count = index_metadata(
        index_dir=args.index_dir,
        chunks_dir=args.chunks_dir,
        use_sqlite=args.sqlite,
        limit=args.limit,
    )

    print(f"Successfully indexed {count} books")
