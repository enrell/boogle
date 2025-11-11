from fastapi.testclient import TestClient

import src.api.main as api_main


class FakeRepository:
    def __init__(self):
        self.storage = {}
        self.upsert_calls = []

    def upsert_book(self, metadata):
        self.upsert_calls.append(metadata)
        self.storage[metadata["book_id"]] = metadata

    def get_book(self, book_id):
        return self.storage.get(book_id)

    def search_books(self, query, limit):
        query = query.lower()
        matches = [
            {"book_id": book_id, "title": data["title"], "url": data["url"]}
            for book_id, data in self.storage.items()
            if query in data["title"].lower() or query in (data.get("author") or "").lower()
        ]
        return matches[:limit]


class FakeScraper:
    def __init__(self):
        self.metadata_map = {}
        self.search_results = []
        self.extract_calls = []
        self.search_calls = []

    def extract_metadata(self, book_id):
        self.extract_calls.append(book_id)
        return self.metadata_map[book_id]

    def search_books(self, query, limit):
        self.search_calls.append((query, limit))
        return self.search_results[:limit]


def make_client(monkeypatch):
    repo = FakeRepository()
    scraper = FakeScraper()
    monkeypatch.setattr(api_main, "database", repo)
    monkeypatch.setattr(api_main, "scraper", scraper)
    client = TestClient(api_main.app)
    return client, repo, scraper


def test_get_metadata_returns_cached_entry(monkeypatch):
    client, repo, scraper = make_client(monkeypatch)
    cached = {
        "book_id": 1,
        "url": "http://example.com/1",
        "title": "Cached Book",
        "author": "Cached Author",
        "files": [],
    }
    repo.upsert_book(cached)

    response = client.get("/metadata/1")

    assert response.status_code == 200
    assert response.json()["title"] == "Cached Book"
    assert scraper.extract_calls == []


def test_get_metadata_fetches_and_caches(monkeypatch):
    client, repo, scraper = make_client(monkeypatch)
    remote = {
        "book_id": 2,
        "url": "http://example.com/2",
        "title": "Remote Book",
        "author": "Remote Author",
        "files": [{"format": "txt", "url": "http://example.com/file.txt"}],
    }
    scraper.metadata_map[2] = remote

    response = client.get("/metadata/2")

    assert response.status_code == 200
    assert response.json()["title"] == "Remote Book"
    assert repo.get_book(2)["title"] == "Remote Book"
    assert repo.upsert_calls[-1]["book_id"] == 2
    assert scraper.extract_calls == [2]


def test_search_combines_cache_with_remote_results(monkeypatch):
    client, repo, scraper = make_client(monkeypatch)
    repo.upsert_book(
        {
            "book_id": 3,
            "url": "http://example.com/3",
            "title": "Local Result",
            "author": "Author",
            "files": [],
        }
    )
    scraper.search_results = [
        {"book_id": 4, "title": "Remote Result", "url": "http://example.com/4"},
    ]
    scraper.metadata_map[4] = {
        "book_id": 4,
        "url": "http://example.com/4",
        "title": "Remote Result",
        "author": "Author",
        "files": [],
    }

    response = client.get("/search", params={"query": "Result", "limit": 2})

    assert response.status_code == 200
    data = response.json()
    returned_ids = {entry["book_id"] for entry in data}
    assert returned_ids == {3, 4}
    assert repo.get_book(4) is not None  # remote metadata cached
    assert scraper.extract_calls == [4]
