from typing import Dict, Iterator, List, Protocol


class SourceClient(Protocol):
    def extract_metadata(self, book_id: str) -> Dict:
        ...

    def search_books(self, query: str, limit: int = 10) -> List[Dict]:
        ...

    def iter_book_ids(self, limit: int | None = None) -> Iterator[str]:
        ...
