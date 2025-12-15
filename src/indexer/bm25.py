import math
import pickle
import re
from array import array
from collections import defaultdict
from pathlib import Path


class BM25Index:
    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.inverted_index: dict[str, array] = {}
        self.doc_lengths = array('I')
        self.doc_metadata: list[dict] = []
        self.avg_doc_length: float = 0.0
        self.num_docs: int = 0
        self._pending: dict[str, list[tuple[int, int]]] = defaultdict(list)

    def tokenize(self, text: str) -> list[str]:
        return re.findall(r'\b[a-z]{2,}\b', text.lower())

    def add_document(self, doc_id: int, text: str, metadata: dict | None = None):
        tokens = self.tokenize(text)
        self.doc_lengths.append(len(tokens))
        self.doc_metadata.append(metadata or {})
        term_freqs: dict[str, int] = defaultdict(int)
        for token in tokens:
            term_freqs[token] += 1
        for term, freq in term_freqs.items():
            self._pending[term].append((doc_id, freq))
        self.num_docs += 1

    def finalize(self):
        total = sum(self.doc_lengths)
        self.avg_doc_length = total / self.num_docs if self.num_docs > 0 else 0.0
        for term, postings in self._pending.items():
            flat = array('I')
            for doc_id, freq in postings:
                flat.append(doc_id)
                flat.append(freq)
            self.inverted_index[term] = flat
        self._pending.clear()

    def idf(self, term: str) -> float:
        postings = self.inverted_index.get(term)
        if not postings:
            return 0.0
        df = len(postings) // 2
        return math.log((self.num_docs - df + 0.5) / (df + 0.5) + 1)

    def _get_tf(self, term: str, doc_id: int) -> int:
        postings = self.inverted_index.get(term)
        if not postings:
            return 0
        for i in range(0, len(postings), 2):
            if postings[i] == doc_id:
                return postings[i + 1]
        return 0

    def score(self, query: str, doc_id: int) -> float:
        tokens = self.tokenize(query)
        doc_length = self.doc_lengths[doc_id]
        score = 0.0
        for term in tokens:
            tf = self._get_tf(term, doc_id)
            if tf == 0:
                continue
            idf = self.idf(term)
            numerator = tf * (self.k1 + 1)
            denominator = tf + self.k1 * (1 - self.b + self.b * doc_length / self.avg_doc_length)
            score += idf * numerator / denominator
        return score

    def search(self, query: str, top_k: int = 10) -> list[tuple[int, float, dict]]:
        tokens = self.tokenize(query)
        candidate_docs: set[int] = set()
        for term in tokens:
            postings = self.inverted_index.get(term)
            if postings:
                for i in range(0, len(postings), 2):
                    candidate_docs.add(postings[i])
        scores = []
        for doc_id in candidate_docs:
            s = self.score(query, doc_id)
            if s > 0:
                scores.append((doc_id, s, self.doc_metadata[doc_id]))
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]

    def save(self, filepath: str | Path):
        self.finalize()
        data = {
            'k1': self.k1,
            'b': self.b,
            'inverted_index': {k: v.tobytes() for k, v in self.inverted_index.items()},
            'doc_lengths': self.doc_lengths.tobytes(),
            'doc_metadata': self.doc_metadata,
            'avg_doc_length': self.avg_doc_length,
            'num_docs': self.num_docs,
        }
        with open(filepath, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(cls, filepath: str | Path) -> 'BM25Index':
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        index = cls(k1=data['k1'], b=data['b'])
        index.inverted_index = {}
        for k, v in data['inverted_index'].items():
            arr = array('I')
            arr.frombytes(v)
            index.inverted_index[k] = arr
        index.doc_lengths = array('I')
        index.doc_lengths.frombytes(data['doc_lengths'])
        index.doc_metadata = data['doc_metadata']
        index.avg_doc_length = data['avg_doc_length']
        index.num_docs = data['num_docs']
        return index
