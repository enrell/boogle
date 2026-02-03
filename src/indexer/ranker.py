"""
Google-like ranking without ML.
BM25 + editorial heuristics using file-based index.
"""
import json
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from rust_bm25 import FileSearcher, analyze
from src.indexer.storage import IndexStorage
from src.indexer.stopwords import load_stopwords

# Tuning parameters
TITLE_BOOST = 3.0
COVERAGE_BOOST = 0.3
LENGTH_NORM = 1000
SUM_TOP_N_WEIGHT = 0.2
REFERENCE_PENALTY = 0.7
STRONG_TITLE_BOOST = 1.3
PHRASE_BOOST = 1.2   # Boost when all query terms in same chunk

REFERENCE_KEYWORDS = {"dictionary", "encyclopedia", "lexicon", "glossary", "index", "catalog", "thesaurus", "concordance"}


@dataclass
class ChunkResult:
    doc_id: int
    book_id: str
    score: float
    title: str
    author: str
    title_tokens: set[str]
    meta: dict = None


@dataclass  
class BookResult:
    book_id: str
    score: float
    title: str
    author: str
    best_chunk_id: int


class Ranker:
    def __init__(self, storage: IndexStorage | None = None, k1: float = 1.5, b: float = 0.75):
        self.storage = storage or IndexStorage()
        self.k1 = k1
        self.b = b
        self._avgdl = 1.0
        self._stopwords = load_stopwords()
        self._searcher: FileSearcher | None = None
        self._index_dir = Path(os.getenv("INDEX_DIR", "data/index"))
        self._load_globals()

    def _load_globals(self):
        """Load avgdl from index.json for scoring adjustments."""
        index_meta_path = self._index_dir / "index.json"
        
        if index_meta_path.exists():
            try:
                with open(index_meta_path) as f:
                    meta = json.load(f)
                self._avgdl = meta.get("avgdl", 1.0)
            except (json.JSONDecodeError, IOError):
                pass

    def _get_searcher(self) -> FileSearcher:
        if self._searcher is None:
            self._searcher = FileSearcher(str(self._index_dir))
            self._searcher.set_stopwords(list(self._stopwords))
        return self._searcher

    def search(self, query: str, top_k: int = 10) -> list[BookResult]:
        query_tokens = [t for t in analyze(query) if t not in self._stopwords]
        if not query_tokens:
            return []
        
        query_set = set(query_tokens)
        
        # 1. Use FileSearcher for BM25 search on file-based index
        searcher = self._get_searcher()
        # Get more candidates for re-ranking (book_id, bm25_score, chunk_id)
        candidates = searcher.search(query, top_k * 20)
        
        if not candidates:
            return []
        
        # 2. Get book metadata for all unique books
        book_ids = list(set(book_id for book_id, _, _ in candidates))
        books_meta = self.storage.get_books_metadata(book_ids)
        
        # 3. Score chunks with boosts
        chunk_results: list[ChunkResult] = []
        for book_id, bm25_score, chunk_id in candidates:
            meta = books_meta.get(book_id, {})
            title = meta.get("title", "")
            author = meta.get("author", "")
            title_tokens = set(meta.get("title_tokens", []))
            
            title_matches = len(query_set & title_tokens)
            title_score = title_matches * 2.0
            
            coverage = title_matches / len(query_set) if query_set else 0
            coverage_mult = 1.0 + COVERAGE_BOOST * coverage
            
            # Phrase boost: if all query terms match title, boost
            phrase_mult = PHRASE_BOOST if title_matches == len(query_set) and len(query_set) > 1 else 1.0
            
            # Length penalty (use avgdl as proxy)
            length_penalty = math.log(1 + LENGTH_NORM / self._avgdl)
            
            score = (bm25_score + TITLE_BOOST * title_score) * coverage_mult * length_penalty * phrase_mult
            
            chunk_results.append(ChunkResult(
                doc_id=chunk_id,
                book_id=book_id,
                score=score,
                title=title,
                author=author,
                title_tokens=title_tokens,
                meta=meta
            ))
        
        # 4. Aggregate chunks by book
        books: dict[str, list[ChunkResult]] = defaultdict(list)
        for cr in chunk_results:
            books[cr.book_id].append(cr)
        
        book_results: list[BookResult] = []
        for book_id, chunks in books.items():
            chunks.sort(key=lambda x: x.score, reverse=True)
            best = chunks[0]
            
            book_score = best.score
            if len(chunks) > 1:
                book_score += SUM_TOP_N_WEIGHT * chunks[1].score
            
            # Reference penalty
            title_lower = best.title.lower()
            if any(kw in title_lower for kw in REFERENCE_KEYWORDS):
                book_score *= REFERENCE_PENALTY
            
            # Strong title match boost
            if query_set and query_set <= best.title_tokens:
                book_score *= STRONG_TITLE_BOOST
            
            # --- Enrichment Boosts ---
            meta = best.meta
            ratings_average = meta.get("ratings_average")
            want_to_read_count = meta.get("want_to_read_count")
            
            # 1. Rating Boost (up to 1.5x)
            if ratings_average and ratings_average > 0:
                # Boost = 1.0 + (rating / 5) * 0.5 -> 5.0 rating gives 1.5x
                rating_mult = 1.0 + (float(ratings_average) / 5.0) * 0.5
                book_score *= rating_mult

            # 2. Popularity Boost (up to 1.2x)
            if want_to_read_count and want_to_read_count > 0:
                # Logarithmic boost
                # 100 -> 1.05, 1000 -> 1.075, 10000 -> 1.1...
                pop_mult = 1.0 + min(0.2, math.log10(max(1, want_to_read_count)) * 0.05)
                book_score *= pop_mult
            # -------------------------
            
            book_results.append(BookResult(
                book_id=book_id,
                score=book_score,
                title=best.title,
                author=best.author,
                best_chunk_id=best.doc_id
            ))
        
        book_results.sort(key=lambda x: x.score, reverse=True)
        
        # 5. Author diversity penalty
        author_counts: dict[str, int] = defaultdict(int)
        for br in book_results:
            n = author_counts[br.author]
            if n > 0:
                br.score *= 0.9 ** n
            author_counts[br.author] += 1
        
        book_results.sort(key=lambda x: x.score, reverse=True)
        return book_results[:top_k]
