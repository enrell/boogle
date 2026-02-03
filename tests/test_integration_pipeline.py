import os
import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from src.pipeline import run_index_pipeline, search
import src.api.main as api_main
from src.db.database import PostgresRepository


import sqlite3
import shutil
from src.pipeline import run_index_pipeline
from src.enrichment.schema import init_db
from src.indexer.ranker import Ranker

def test_full_pipeline(test_db_env, test_dirs, request):
    """
    Sequential Integration Test for Boogle Pipeline:
    1. Scrape & Download & Index (No Enrichment)
    2. Search -> Record Scores
    3. Setup Mock Enrichment Data
    4. Enrich
    5. Search -> Verify Score Increase
    """
    use_sqlite = request.config.getoption("--sqlite")
    
    # --- Step 1: Scrape & Download & Index (No Enrichment) ---
    print(f"\n[TEST] Step 1: Running Index Pipeline (SQLite={use_sqlite})...")
    
    # use_sqlite argument must match our env setup
    # Limit default to a small number
    run_index_pipeline(
        limit=5,
        batch_size=5,
        use_sqlite=use_sqlite,
        reindex=True,
        workers=4,
        enrich=False
    )
    
    # Verify DB content
    from src.db.database import PostgresRepository
    db = PostgresRepository(use_sqlite=use_sqlite)
    
    # --- Step 2: Record Baseline Scores ---
    print("\n[TEST] Step 2: Baseline Search...")
    ranker = Ranker(k1=1.5, b=0.75)
    
    # Search for a term likely to return results. 
    # "The" is safe, or "Project" if Gutenberg.
    query = "Project" 
    results_baseline = ranker.search(query, top_k=10)
    
    if not results_baseline:
        # Try another query if "Project" fails (unlikely for Gutenberg)
        query = "the"
        results_baseline = ranker.search(query, top_k=10)
        
    assert len(results_baseline) > 0, "Baseline search returned no results"
    
    baseline_scores = {r.book_id: r.score for r in results_baseline}
    top_book_id = results_baseline[0].book_id
    top_book_title = results_baseline[0].title
    print(f"Baseline Top Hit: {top_book_title} (ID: {top_book_id}) Score: {baseline_scores[top_book_id]}")
    
    # --- Step 3: Setup Mock Enrichment Data ---
    print("\n[TEST] Step 3: Setting up Enrichment Data...")
    ol_db_path = "data/openlibrary.db"
    if os.path.exists(ol_db_path):
         shutil.move(ol_db_path, ol_db_path + ".bak")
         
    try:
        init_db(ol_db_path)
        conn = sqlite3.connect(ol_db_path)
        c = conn.cursor()
        
        # Insert a STRONG signal for the top book found
        # We need the title exactly as found in the index/DB for exact match lookup
        # (Our mock logic matches title match)
        
        # Clean title for the mock insert if needed, but client strips special chars.
        # Let's insert exact title.
        c.execute("""
            INSERT INTO works (key, title, authors, ratings_average, ratings_count, want_to_read_count, edition_count, subjects)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, ("OL_TEST_1", top_book_title, '["Unknown"]', 5.0, 1000, 10000, 50, '["Test"]'))
        
        c.execute("INSERT INTO works_fts(works_fts) VALUES('rebuild')")
        conn.commit()
        conn.close()
        
        # --- Step 4: Run Enrichment ---
        print("\n[TEST] Step 4: Running Enrichment...")
        
        # We can run the pipeline with enrich=True. 
        # Since we already seeded, this should just hit the enrich step and re-index?
        # Actually run_index_pipeline re-runs seeding (fast if done) then enrich then index.
        # We want to make sure the ranker picks up the new data. Reading from DB happens at search time?
        # Ranker.search() calls storage.get_books_metadata() which queries DB.
        # So we just need to update the DB rows.
        
        run_index_pipeline(
            limit=5,
            batch_size=5,
            use_sqlite=use_sqlite,
            reindex=False, # No need to re-index, just metadata update
            workers=4,
            enrich=True
        )
        
        # --- Step 5: Verify Score Increase ---
        print("\n[TEST] Step 5: Enriched Search Comparison...")
        
        # Re-init ranker ? No, storage queries DB. 
        # But storage might cache metadata? 
        # CACHE_MAX_BOOKS LRU cache in storage.py... we might need to clear it or create new ranker.
        # Ranker creates new storage if not provided?
        ranker_enhanced = Ranker(k1=1.5, b=0.75) 
        
        results_enhanced = ranker_enhanced.search(query, top_k=10)
        enhanced_scores = {r.book_id: r.score for r in results_enhanced}
        
        print(f"Enhanced Top Hit: {results_enhanced[0].title} (ID: {results_enhanced[0].book_id}) Score: {results_enhanced[0].score}")
        
        # Verify the top book got a boost
        old_score = baseline_scores.get(top_book_id, 0)
        new_score = enhanced_scores.get(top_book_id, 0)
        
        print(f"Comparison for {top_book_title}: {old_score:.4f} -> {new_score:.4f}")
        
        assert new_score > old_score, f"Score did not increase after enrichment! ({old_score} -> {new_score})"
        
    finally:
        # Clean up mock DB
        if os.path.exists(ol_db_path):
             os.remove(ol_db_path)
        if os.path.exists(ol_db_path + ".bak"):
             shutil.move(ol_db_path + ".bak", ol_db_path)
        db.close()

