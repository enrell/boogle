from typing import Mapping, Optional

from src.db import PostgresRepository
from src.sources.types import SourceClient


class SeedService:
    def __init__(self, repository: PostgresRepository, sources: Mapping[str, SourceClient]):
        self.repository = repository
        self.sources = sources

    def seed(self, source: Optional[str] = None, limit: Optional[int] = None) -> None:
        targets = [source] if source else list(self.sources.keys())
        for name in targets:
            self._seed_source(name, limit)

    def _seed_source(self, name: str, limit: Optional[int]) -> None:
        client = self.sources.get(name)
        if not client:
            raise ValueError(f"Unsupported source {name}")
        count = 0
        for book_id in client.iter_book_ids(limit):
            try:
                metadata = client.extract_metadata(book_id)
                self.repository.upsert_book(metadata)
                count += 1
            except Exception:
                continue
            if limit and count >= limit:
                break
