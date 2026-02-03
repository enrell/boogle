
import os
import shutil
import tempfile
import json
import time
from pathlib import Path

# Adjust path to find rust_bm25 if needed
import sys
sys.path.append(os.getcwd())

try:
    from rust_bm25 import index_corpus_file, RealTimeIndexer
except ImportError:
    print("Could not import rust_bm25. Ensure you are running from project root and it is installed/built.")
    sys.exit(1)

def create_dummy_books(books_dir):
    """Creates dummy text files to simulate a corpus."""
    print(f"[Setup] Creating dummy books in {books_dir}...")
    Path(books_dir).mkdir(parents=True, exist_ok=True)
    
    books = [
        ("book1.txt", "The quick brown fox jumps over the lazy dog."),
        ("book2.txt", "Rust is a systems programming language that runs blazingly fast."),
        ("book3.txt", "Near real-time indexing allows immediate search availability."),
    ]
    
    for filename, content in books:
        with open(os.path.join(books_dir, filename), "w") as f:
            f.write(content)

def test_pipeline():
    # Use a local directory for debugging visibility
    temp_dir = os.path.abspath("test_pipeline_env")
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    
    try:
        print(f"--- Running Isolated Pipeline Test in {temp_dir} ---")
        
        books_dir = os.path.join(temp_dir, "books")
        index_dir = os.path.join(temp_dir, "index")
        chunks_dir = os.path.join(temp_dir, "chunks")
        
        # 1. Setup Data
        create_dummy_books(books_dir)
        
        # 2. Run Indexing
        print("\n--- Step 1: Batch Indexing (Disk) ---")
        stopwords = ["the", "is", "a", "that"]
        
        # Check analysis
        try:
            from rust_bm25 import analyze
            print(f"[Debug] Analyze 'The quick brown fox': {analyze('The quick brown fox')}")
        except:
            pass

        indexed_count, total_chunks = index_corpus_file(
            books_dir,
            index_dir,
            chunks_dir,
            stopwords, # strict list
            100, # chunk size
            10,  # overlap
            10   # batch size
        )
        print(f"[Index] Indexed {indexed_count} books, {total_chunks} chunks.")
        assert indexed_count == 3
        
        # 3. Initialize RealTimeIndexer (NRT)
        print("\n--- Step 2: NRT Search Initialization ---")
        # NRT indexer loads the disk index we just created
        indexer = RealTimeIndexer(index_dir)
        
        # 4. Verify Disk Search
        print("[Search] Querying disk data...")
        results = indexer.search("brown", 10)
        print(f"Results for 'brown': {results}")
        assert len(results) > 0
        assert "book1" in str(results) # Assuming ID or metadata maps back, or we check doc ID logic
        
        # 5. Verify NRT (Add new doc in memory)
        print("\n--- Step 3: NRT Ingestion & Search ---")
        new_doc_content = "This is a new book about isolated testing pipelines."
        new_doc_meta = json.dumps({"title": "Test Book 4", "author": "Tester"})
        
        start = time.time()
        indexer.add_document(new_doc_content, new_doc_meta)
        print(f"[NRT] Added document in {time.time() - start:.4f}s")
        
        print("[Search] Querying for 'isolated' (NRT)...")
        nrt_results = indexer.search("isolated", 10)
        print(f"Results for 'isolated': {nrt_results}")
        assert len(nrt_results) > 0
        
        # 6. Verify Combined Search
        print("\n--- Step 4: Combined Search ---")
        # "Rust" is in disk book2, "pipelines" is in RAM book4.
        # Searching for something common? Or separate queries.
        # Search "search" -> book3 (disk)
        res_disk = indexer.search("search", 10) 
        assert len(res_disk) > 0
        print(f"Found 'search' in disk doc: {res_disk}")

        print("\n[Success] Pipeline test completed without touching production data.")


    except Exception as e:
        print(f"\n[Error] Pipeline test failed: {e}")
        raise e
    finally:
        # Cleanup
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"[Cleanup] Removed {temp_dir}")

if __name__ == "__main__":
    test_pipeline()
