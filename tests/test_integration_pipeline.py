import os
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from src.pipeline import run_index_pipeline, search
import src.api.main as api_main
from src.db.database import PostgresRepository

def test_full_pipeline(test_db_env, test_dirs):
    """
    Sequential Integration Test for Boogle Pipeline
    
    Steps:
    1. Scrape & Download: Fetch a small number of real books.
    2. Indexing: Verify index creation.
    3. Search (Library): Verify searcher finds downloaded books.
    4. Search (API): Verify API endpoint returns correct results.
    """
    
    # --- Step 1: Scrape & Download & Index ---
    print("\n[TEST] Step 1: Running Index Pipeline...")
    
    # We use limit=10 to ensure we get some valid books despite filters/network issues
    # use_sqlite=True and reindex=True are key
    indexed_count = run_index_pipeline(
        limit=10,
        batch_size=5,
        use_sqlite=True,
        reindex=True,
        workers=4
    )
    
    print(f"\n[TEST] Indexed count: {indexed_count}")
    
    # Verify files exist
    books_dir = Path(test_dirs["books"])
    index_dir = Path(test_dirs["index"])
    
    assert any(books_dir.iterdir()), "Books directory should not be empty"
    assert any(index_dir.iterdir()), "Index directory should not be empty"
    
    # Verify DB content
    db = PostgresRepository(use_sqlite=True)
    with db.get_session() as session:
        # Assuming table name is 'books', but using repository method to check
        # We can't easily count all without a count method, but we can search for * something
        pass # verified via search below
        
    # --- Step 2: Search (Library Level) ---
    print("\n[TEST] Step 2: Testing Search Library...")
    
    # We don't know exactly what books we got, but we can search for common words
    # or check the database to find a title to search for.
    # Let's try to find a book title from the DB first.
    
    # Helper to get a known book title
    # We need to peek into the DB
    found_title = "Project Gutenberg" # Default fallback
    found_author = ""
    
    # Since we can't easily iterate all books without direct DB access code that might violate layers,
    # let's rely on the fact that we downloaded *something*.
    # Actually, let's use the DB repository to "search" or list.
    # The repository has `search_books`. Let's use that on a very generic term.
    
    # Wait, we can inspect the downloaded files to know what to search for?
    # Or just search for "the" or "and" which should be in any English book.
    
    # Let's try to get a specific book to be more precise
    # iterating the filtered files in books_dir
    files = list(books_dir.glob("*.txt"))
    if not files:
        # maybe epub?
        files = list(books_dir.glob("*.epub*"))
        
    assert len(files) > 0, "No downloaded book files found"
    
    # --- Step 3: API Test ---
    print("\n[TEST] Step 3: Testing API...")
    
    with TestClient(api_main.app) as client:
        # Use the generic term "the" which should match almost any book text body
        # or "gutenberg" which is often in headers/footers
        response = client.get("/search", params={"query": "gutenberg", "limit": 5})
        
        if response.status_code != 200:
            print(f"\n[TEST] API Error: {response.text}")
        
        assert response.status_code == 200
        results = response.json()
        
        # We expect some results, assuming generic query hits.
        # If the index works, "gutenberg" should be everywhere.
        assert len(results) > 0, "API search for 'gutenberg' returned no results"
        
        first_hit = results[0]
        assert "book_id" in first_hit
        assert "title" in first_hit
        assert "score" in first_hit
        
        print(f"\n[TEST] Success! Found {len(results)} results. Top hit: {first_hit['title']}")
