import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock

import src.api.main as api_main


class FakeRepository:
    def __init__(self):
        self.storage = {}

    def get_book(self, source, book_id):
        # We only support gutenberg in current API implementation for search results
        if source == "gutenberg":
            return self.storage.get(book_id)
        return None

    def seed_book(self, book_id, title, author):
        self.storage[book_id] = {
            "source": "gutenberg",
            "book_id": book_id,
            "title": title,
            "author": author,
            "url": f"http://example.com/{book_id}",
            "files": []
        }


class FakeSearcher:
    def __init__(self, index_dir):
        self.results = []

    def set_stopwords(self, stopwords):
        pass

    def search(self, query, limit):
        # Returns list of (book_id, score, chunk_id)
        # Filter results that match query if we wanted to be fancy,
        # but for mocking we just return pre-configured results
        return self.results[:limit]


@pytest.fixture
def api_client(monkeypatch):
    repo = FakeRepository()
    searcher = FakeSearcher("dummy_dir")
    
    # Patch the global variables in api.main to point to our mocks
    monkeypatch.setattr(api_main, "database", repo)
    monkeypatch.setattr(api_main, "searcher", searcher)
    
    # Override lifespan to prevent real DB connection/Index loading
    from contextlib import asynccontextmanager
    @asynccontextmanager
    async def mock_lifespan(app):
        # We manually set the globals above, so we don't need lifespan to do anything
        yield
        
    original_lifespan = api_main.app.router.lifespan_context
    api_main.app.router.lifespan_context = mock_lifespan
    
    with TestClient(api_main.app) as client:
        yield client, repo, searcher
        
    # Restore (though usually not strictly necessary if process dies, but good practice)
    api_main.app.router.lifespan_context = original_lifespan


def test_health_check(api_client):
    client, _, _ = api_client
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


def test_get_metadata_found(api_client):
    client, repo, _ = api_client
    repo.seed_book("123", "Test Book", "Test Author")
    
    response = client.get("/metadata/gutenberg/123")
    assert response.status_code == 200
    data = response.json()
    assert data["book_id"] == "123"
    assert data["title"] == "Test Book"


def test_get_metadata_not_found(api_client):
    client, _, _ = api_client
    response = client.get("/metadata/gutenberg/999")
    assert response.status_code == 404


def test_search_returns_ranked_results(api_client):
    client, repo, searcher = api_client
    
    # Setup Data
    repo.seed_book("1", "Python Guide", "Guido")
    repo.seed_book("2", "Rust Guide", "Ferris")
    
    # Setup Mock Search Results: (book_id, score, chunk_id)
    # Book 2 has higher score
    searcher.results = [
        ("2", 0.95, 10),
        ("1", 0.80, 5),
    ]
    
    response = client.get("/search", params={"query": "guide", "limit": 10})
    
    assert response.status_code == 200
    results = response.json()
    
    assert len(results) == 2
    assert results[0]["book_id"] == "2"
    assert results[0]["title"] == "Rust Guide"
    assert results[0]["score"] == 0.95 * 1.5
    
    assert results[1]["book_id"] == "1"
    assert results[1]["title"] == "Python Guide"


def test_search_filters_unknown_books(api_client):
    client, repo, searcher = api_client
    
    # Only Book 1 is in DB
    repo.seed_book("1", "Python Guide", "Guido")
    
    # Searcher returns Book 1 and Book 999 (which is missing from DB)
    searcher.results = [
        ("1", 0.80, 5),
        ("999", 0.99, 20),
    ]
    
    response = client.get("/search", params={"query": "guide"})
    results = response.json()
    
    # Should only return Book 1
    assert len(results) == 1
    assert results[0]["book_id"] == "1"


def test_search_deduplicates_chunks(api_client):
    client, repo, searcher = api_client
    repo.seed_book("1", "Dedupe Me", "Author")
    
    # Searcher returns multiple chunks for same book
    searcher.results = [
        ("1", 0.5, 100),
        ("1", 0.9, 200), # Higher score should win
        ("1", 0.2, 300),
    ]
    
    response = client.get("/search", params={"query": "test"})
    results = response.json()
    
    assert len(results) == 1
    assert results[0]["score"] == 0.9
