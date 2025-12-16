import heapq
import json
import math
from dataclasses import dataclass, field

from rust_bm25 import analyze, decode_postings
from src.indexer.storage import IndexStorage
from src.indexer.stopwords import load_stopwords


@dataclass 
class TermData:
    idf: float
    upper_bound: float
    postings: dict[int, int] = field(default_factory=dict)


class PgBM25Index:
    def __init__(self, storage: IndexStorage | None = None, k1: float = 1.5, b: float = 0.75):
        self.storage = storage or IndexStorage()
        self.k1 = k1
        self.b = b
        self._num_docs = 0
        self._avgdl = 1.0
        self._load_globals()

    def _load_globals(self):
        n = self.storage.get_global("num_docs")
        avgdl = self.storage.get_global("avgdl")
        if n:
            self._num_docs = int(n)
        if avgdl:
            self._avgdl = float(avgdl)

    def _idf(self, df: int) -> float:
        return math.log((self._num_docs - df + 0.5) / (df + 0.5) + 1)

    def _bm25_term_score(self, tf: int, idf: float, doc_len: int) -> float:
        numerator = tf * (self.k1 + 1)
        denominator = tf + self.k1 * (1 - self.b + self.b * doc_len / self._avgdl)
        return idf * numerator / denominator

    def _bm25_upper_bound(self, idf: float) -> float:
        return idf * (self.k1 + 1)

    def search(self, query: str, top_k: int = 10) -> list[tuple[int, float, dict]]:
        stopwords = load_stopwords()
        tokens = [t for t in analyze(query) if t not in stopwords]
        if not tokens:
            return []
        
        terms: list[TermData] = []
        
        for token in set(tokens):
            term_data = self.storage.get_term(token)
            if term_data:
                df, postings_blob = term_data
                idf = self._idf(df)
                postings = {doc_id: tf for doc_id, tf in decode_postings(postings_blob)}
                terms.append(TermData(
                    idf=idf,
                    upper_bound=self._bm25_upper_bound(idf),
                    postings=postings
                ))
        
        if not terms:
            return []
        
        terms.sort(key=lambda t: len(t.postings))
        
        candidate_docs = set(terms[0].postings.keys())
        for term in terms[1:]:
            candidate_docs &= set(term.postings.keys())
            if len(candidate_docs) <= top_k * 10:
                break
        
        if not candidate_docs:
            candidate_docs = set(terms[0].postings.keys())
            if len(candidate_docs) > 50000:
                scored_candidates = []
                for doc_id in terms[0].postings:
                    upper = sum(t.upper_bound for t in terms if doc_id in t.postings)
                    scored_candidates.append((upper, doc_id))
                scored_candidates.sort(reverse=True)
                candidate_docs = set(doc_id for _, doc_id in scored_candidates[:50000])
        
        doc_data = self.storage.get_documents_batch(list(candidate_docs))
        
        top_heap: list[tuple[float, int]] = []
        threshold = 0.0
        
        candidates_with_upper = []
        for doc_id in candidate_docs:
            if doc_id not in doc_data:
                continue
            upper = sum(t.upper_bound for t in terms if doc_id in t.postings)
            candidates_with_upper.append((upper, doc_id))
        
        candidates_with_upper.sort(reverse=True)
        
        for upper, doc_id in candidates_with_upper:
            if upper <= threshold and len(top_heap) >= top_k:
                break
            
            doc_len = doc_data[doc_id][0]
            score = sum(
                self._bm25_term_score(t.postings[doc_id], t.idf, doc_len)
                for t in terms if doc_id in t.postings
            )
            
            if len(top_heap) < top_k:
                heapq.heappush(top_heap, (score, doc_id))
                if len(top_heap) == top_k:
                    threshold = top_heap[0][0]
            elif score > threshold:
                heapq.heapreplace(top_heap, (score, doc_id))
                threshold = top_heap[0][0]
        
        top_heap.sort(reverse=True)
        
        results = []
        for score, doc_id in top_heap:
            if doc_id in doc_data:
                _, meta_str = doc_data[doc_id]
                try:
                    meta = json.loads(meta_str) if meta_str else {}
                except:
                    meta = {}
                results.append((doc_id, score, meta))
        
        return results

    @property
    def num_docs(self) -> int:
        return self._num_docs
