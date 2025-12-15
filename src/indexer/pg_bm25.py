import json
import math
from collections import defaultdict

from rust_bm25 import analyze, encode_postings, decode_postings
from src.indexer.storage import IndexStorage


class PgBM25Index:
    def __init__(self, storage: IndexStorage | None = None, k1: float = 1.5, b: float = 0.75, flush_every: int = 20000):
        self.storage = storage or IndexStorage()
        self.k1 = k1
        self.b = b
        self.flush_every = flush_every
        self._pending_docs: list[tuple[int, int, str]] = []
        self._pending_terms: dict[str, list[tuple[int, int]]] = defaultdict(list)
        self._total_length = 0
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

    def add_document(self, doc_id: int, text: str, metadata: str):
        tokens = analyze(text)
        doc_length = len(tokens)
        self._pending_docs.append((doc_id, doc_length, metadata))
        self._total_length += doc_length
        
        term_freqs: dict[str, int] = defaultdict(int)
        for token in tokens:
            term_freqs[token] += 1
        
        for term, freq in term_freqs.items():
            self._pending_terms[term].append((doc_id, freq))
        
        if len(self._pending_docs) >= self.flush_every:
            self._flush()

    def _flush(self):
        if self._pending_docs:
            self.storage.insert_documents_batch(self._pending_docs)
            self._num_docs += len(self._pending_docs)
            self._pending_docs = []
        
        if self._pending_terms:
            terms_batch = []
            for term, postings in self._pending_terms.items():
                encoded = encode_postings(postings)
                terms_batch.append((term, len(postings), encoded))
            self.storage.insert_terms_batch(terms_batch, merge=True)
            self._pending_terms.clear()

    def finalize(self):
        self._flush()
        self._avgdl = self._total_length / self._num_docs if self._num_docs > 0 else 0
        self.storage.set_global("num_docs", str(self._num_docs))
        self.storage.set_global("avgdl", str(self._avgdl))
        self.storage.set_global("k1", str(self.k1))
        self.storage.set_global("b", str(self.b))

    def _idf(self, df: int) -> float:
        return math.log((self._num_docs - df + 0.5) / (df + 0.5) + 1)

    def _bm25_score(self, tf: int, idf: float, doc_len: int) -> float:
        numerator = tf * (self.k1 + 1)
        denominator = tf + self.k1 * (1 - self.b + self.b * doc_len / self._avgdl)
        return idf * numerator / denominator

    def search(self, query: str, top_k: int = 10) -> list[tuple[int, float, dict]]:
        tokens = analyze(query)
        terms_data = self.storage.get_terms_batch(tokens)
        
        candidates: dict[int, list[tuple[int, float]]] = defaultdict(list)
        doc_ids_needed: set[int] = set()
        
        for token in tokens:
            if token not in terms_data:
                continue
            df, postings_blob = terms_data[token]
            idf = self._idf(df)
            postings = decode_postings(postings_blob)
            for doc_id, tf in postings:
                doc_ids_needed.add(doc_id)
                candidates[doc_id].append((tf, idf))
        
        doc_data = self.storage.get_documents_batch(list(doc_ids_needed))
        
        results = []
        for doc_id, tf_idf_pairs in candidates.items():
            if doc_id not in doc_data:
                continue
            doc_len, meta_str = doc_data[doc_id]
            score = sum(self._bm25_score(tf, idf, doc_len) for tf, idf in tf_idf_pairs)
            try:
                meta = json.loads(meta_str) if meta_str else {}
            except:
                meta = {}
            results.append((doc_id, score, meta))
        
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]

    def clear(self):
        self.storage.clear()
        self._num_docs = 0
        self._avgdl = 0.0
        self._total_length = 0
        self._pending_docs = []
        self._pending_terms.clear()

    @property
    def num_docs(self) -> int:
        return self._num_docs
