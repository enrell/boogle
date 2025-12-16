"""
Google-like ranking without ML.
BM25 + editorial heuristics.
"""
import math
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache

from rust_bm25 import WandSearcher, analyze
from src.indexer.storage import IndexStorage
from src.indexer.stopwords import load_stopwords

# Tuning parameters
TITLE_BOOST = 3.0
COVERAGE_BOOST = 0.3
LENGTH_NORM = 1000
SUM_TOP_N_WEIGHT = 0.2
REFERENCE_PENALTY = 0.7
STRONG_TITLE_BOOST = 1.3
DF_THRESHOLD = 0.90  # Ignore terms appearing in >90% of docs
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
        self._num_docs = 0
        self._avgdl = 1.0
        self._stopwords = load_stopwords()
        self._searcher: WandSearcher | None = None
        self._load_globals()

    def _load_globals(self):
        n = self.storage.get_global("num_docs")
        avgdl = self.storage.get_global("avgdl")
        if n:
            self._num_docs = int(n)
        if avgdl:
            self._avgdl = float(avgdl)

    def _get_searcher(self) -> WandSearcher:
        if self._searcher is None:
            self._searcher = WandSearcher(self._num_docs, self._avgdl, self.k1, self.b)
            self._searcher.set_stopwords(list(self._stopwords))
        return self._searcher

    def _is_dynamic_stopword(self, df: int) -> bool:
        """Term is too common (>90% of docs)."""
        return self._num_docs > 0 and df / self._num_docs > DF_THRESHOLD

    @lru_cache(maxsize=1000)
    def _get_term_cached(self, term: str) -> tuple[int, bytes] | None:
        """Cached posting list lookup."""
        return self.storage.get_term(term)

    def search(self, query: str, top_k: int = 10) -> list[BookResult]:
        query_tokens = [t for t in analyze(query) if t not in self._stopwords]
        if not query_tokens:
            return []
        
        query_set = set(query_tokens)
        
        # 1. Get posting lists, filter dynamic stopwords
        posting_data = []
        common_terms_data = [] # Fallback if everything is filtered
        filtered_terms = set()
        
        for token in query_set:
            term_data = self._get_term_cached(token)
            if term_data:
                df, postings_blob = term_data
                # Skip terms that appear in >DF_THRESHOLD of docs
                if self._is_dynamic_stopword(df):
                    filtered_terms.add(token)
                    common_terms_data.append((df, postings_blob))
                    continue
                posting_data.append((df, postings_blob))
        
        # Fallback: if all terms were filtered (e.g. specialized corpus or very common terms), use them anyway
        if not posting_data and common_terms_data:
            posting_data = common_terms_data
            filtered_terms.clear()

        if not posting_data:
            return []
        
        # Update query_set to exclude filtered terms for coverage calc
        query_set -= filtered_terms
        
        # 2. WAND search (doc_lengths computed internally)
        searcher = self._get_searcher()
        candidates = searcher.search("", posting_data, top_k * 20)
        
        if not candidates:
            return []
        
        # 3. Get chunk -> book mapping
        chunk_ids = [doc_id for doc_id, _ in candidates]
        chunk_books = self.storage.get_chunks_batch(chunk_ids)
        
        # 4. Get book metadata
        book_ids = list(set(chunk_books.values()))
        books_meta = self.storage.get_books_metadata(book_ids)
        
        # 5. Score chunks with boosts
        chunk_results: list[ChunkResult] = []
        for doc_id, bm25_score in candidates:
            book_id = chunk_books.get(doc_id)
            if not book_id:
                continue
            
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
                doc_id=doc_id,
                book_id=book_id,
                score=score,
                title=title,
                author=author,
                title_tokens=title_tokens
            ))
        
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
            
            book_results.append(BookResult(
                book_id=book_id,
                score=book_score,
                title=best.title,
                author=best.author,
                best_chunk_id=best.doc_id
            ))
        
        book_results.sort(key=lambda x: x.score, reverse=True)
        
        author_counts: dict[str, int] = defaultdict(int)
        for br in book_results:
            n = author_counts[br.author]
            if n > 0:
                br.score *= 0.9 ** n
            author_counts[br.author] += 1
        
        book_results.sort(key=lambda x: x.score, reverse=True)
        return book_results[:top_k]
