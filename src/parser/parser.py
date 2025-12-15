import re
from pathlib import Path
from typing import Iterator

import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup


class EpubParser:
    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 100):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def load_epub(self, filepath: str | Path) -> epub.EpubBook | None:
        try:
            return epub.read_epub(str(filepath), options={'ignore_ncx': True})
        except Exception:
            return None

    def extract_text(self, book: epub.EpubBook) -> str:
        texts = []
        for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
            html = item.get_content().decode('utf-8', errors='ignore')
            soup = BeautifulSoup(html, 'html.parser')
            for tag in soup(['script', 'style', 'nav', 'header', 'footer']):
                tag.decompose()
            text = soup.get_text(separator=' ', strip=True)
            text = re.sub(r'\s+', ' ', text)
            if text:
                texts.append(text)
        return ' '.join(texts)

    def chunk_text(self, text: str) -> list[str]:
        if len(text) <= self.chunk_size:
            return [text] if text else []
        chunks = []
        start = 0
        while start < len(text):
            end = start + self.chunk_size
            if end < len(text):
                space_idx = text.rfind(' ', start, end)
                if space_idx > start:
                    end = space_idx
            chunks.append(text[start:end].strip())
            start = end - self.chunk_overlap
        return [c for c in chunks if c]

    def process_epub(self, filepath: str | Path) -> Iterator[str]:
        book = self.load_epub(filepath)
        if not book:
            return
        text = self.extract_text(book)
        yield from self.chunk_text(text)

    def process_epub_to_text(self, filepath: str | Path) -> str | None:
        book = self.load_epub(filepath)
        if not book:
            return None
        return self.extract_text(book)
