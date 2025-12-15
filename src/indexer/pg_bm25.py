import heapq
import json
import math

from rust_bm25 import analyze, decode_postings
from src.indexer.storage import IndexStorage

STOPWORDS = {'the', 'be', 'to', 'of', 'and', 'in', 'that', 'have', 'it', 'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at', 'this', 'but', 'his', 'by', 'from', 'they', 'we', 'say', 'her', 'she', 'or', 'an', 'will', 'my', 'one', 'all', 'would', 'there', 'their', 'what', 'so', 'up', 'out', 'if', 'about', 'who', 'get', 'which', 'go', 'me', 'is', 'are', 'was', 'were', 'been', 'being', 'has', 'had', 'does', 'did', 'a', 'am'}


class PgBM25Index:
    def __init__(self, storage: IndexStorage | None = None, k1: float = 1.5, b: float = 0.75):
        self.storage = storage or IndexStorage()
        self.k1 = k1
        self.b = b
        self._num_docs = 0
        self._avgdl = 0.0
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

    def search(self, query: str, top_k: int = 10) -> list[tuple[int, float, dict]]:
        tokens = [t for t in analyze(query) if t not in STOPWORDS]
        if not tokens:
            return []
        
        term_info = []
        for token in tokens:
            term_data = self.storage.get_term(token)
            if term_data:
                df, postings_blob = term_data
                term_info.append((token, df, self._idf(df), postings_blob))
        
        if not term_info:
            return []
        
        term_info.sort(key=lambda x: x[1])
        
        scores: dict[int, float] = {}
        doc_lengths_cache: dict[int, int] = {}
        
        for token, df, idf, postings_blob in term_info:
            postings = decode_postings(postings_blob)
            
            for doc_id, tf in postings:
                if doc_id not in doc_lengths_cache:
                    doc = self.storage.get_document(doc_id)
                    if doc:
                        doc_lengths_cache[doc_id] = doc[0]
                    else:
                        continue
                
                score = self._bm25_term_score(tf, idf, doc_lengths_cache[doc_id])
                scores[doc_id] = scores.get(doc_id, 0.0) + score
        
        top_docs = heapq.nlargest(top_k, scores.items(), key=lambda x: x[1])
        
        results = []
        for doc_id, score in top_docs:
            doc = self.storage.get_document(doc_id)
            if doc:
                _, meta_str = doc
                try:
                    meta = json.loads(meta_str) if meta_str else {}
                except:
                    meta = {}
                results.append((doc_id, score, meta))
        
        return results

    @property
    def num_docs(self) -> int:
        return self._num_docs
