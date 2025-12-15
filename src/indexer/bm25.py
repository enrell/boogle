import math
import pickle
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterator


class BM25Index:
    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.inverted_index: dict[str, list[tuple[int, int]]] = defaultdict(list)
        self.doc_lengths: list[int] = []
        self.doc_metadata: list[dict] = []
        self.avg_doc_length: float = 0.0
        self.num_docs: int = 0

    def tokenize(self, text: str) -> list[str]:
        text = text.lower()
        tokens = re.findall(r'\b[a-z]{2,}\b', text)
        return tokens

    def add_document(self, doc_id: int, text: str, metadata: dict | None = None):
        tokens = self.tokenize(text)
        doc_length = len(tokens)
        self.doc_lengths.append(doc_length)
        self.doc_metadata.append(metadata or {})
        term_freqs: dict[str, int] = defaultdict(int)
        for token in tokens:
            term_freqs[token] += 1
        for term, freq in term_freqs.items():
            self.inverted_index[term].append((doc_id, freq))
        self.num_docs += 1
        total_length = sum(self.doc_lengths)
        self.avg_doc_length = total_length / self.num_docs if self.num_docs > 0 else 0.0

    def idf(self, term: str) -> float:
        df = len(self.inverted_index.get(term, []))
        if df == 0:
            return 0.0
        return math.log((self.num_docs - df + 0.5) / (df + 0.5) + 1)

    def score(self, query: str, doc_id: int) -> float:
        tokens = self.tokenize(query)
        doc_length = self.doc_lengths[doc_id]
        score = 0.0
        for term in tokens:
            postings = self.inverted_index.get(term, [])
            tf = 0
            for did, freq in postings:
                if did == doc_id:
                    tf = freq
                    break
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
            for doc_id, _ in self.inverted_index.get(term, []):
                candidate_docs.add(doc_id)
        scores = []
        for doc_id in candidate_docs:
            s = self.score(query, doc_id)
            if s > 0:
                scores.append((doc_id, s, self.doc_metadata[doc_id]))
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]

    def save(self, filepath: str | Path):
        data = {
            'k1': self.k1,
            'b': self.b,
            'inverted_index': dict(self.inverted_index),
            'doc_lengths': self.doc_lengths,
            'doc_metadata': self.doc_metadata,
            'avg_doc_length': self.avg_doc_length,
            'num_docs': self.num_docs,
        }
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)

    @classmethod
    def load(cls, filepath: str | Path) -> 'BM25Index':
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        index = cls(k1=data['k1'], b=data['b'])
        index.inverted_index = defaultdict(list, data['inverted_index'])
        index.doc_lengths = data['doc_lengths']
        index.doc_metadata = data['doc_metadata']
        index.avg_doc_length = data['avg_doc_length']
        index.num_docs = data['num_docs']
        return index


def index_corpus(
    epub_dir: str | Path,
    index_path: str | Path,
    chunk_size: int = 1000,
    chunk_overlap: int = 100
) -> BM25Index:
    from src.parser.parser import EpubParser
    
    epub_dir = Path(epub_dir)
    parser = EpubParser(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    index = BM25Index()
    doc_id = 0
    
    for epub_file in epub_dir.glob("*.epub"):
        book_id = epub_file.stem
        for chunk in parser.process_epub(epub_file):
            metadata = {'book_id': book_id, 'chunk_id': doc_id}
            index.add_document(doc_id, chunk, metadata)
            doc_id += 1
    
    index.save(index_path)
    return index
