import rust_bm25
import time
import os
import shutil

INDEX_DIR = "test_nrt_index"

def cleanup():
    if os.path.exists(INDEX_DIR):
        shutil.rmtree(INDEX_DIR)
    os.makedirs(INDEX_DIR)
    # create dummy index.json
    with open(os.path.join(INDEX_DIR, "index.json"), "w") as f:
        f.write('{"segments": [], "total_docs": 0, "avgdl": 0.0}')

def test_nrt():
    cleanup()
    print("Initializing RealTimeIndexer...")
    indexer = rust_bm25.RealTimeIndexer(INDEX_DIR)
    
    print("Searching empty index...")
    results = indexer.search("hello", 10)
    print(f"Results: {results}")
    assert len(results) == 0

    print("Adding document 'Hello World'...")
    start = time.time()
    indexer.add_document("Hello World", '{"title": "Test Doc 1"}')
    print(f"Added in {time.time() - start:.4f}s")

    print("Searching immediately...")
    start = time.time()
    results = indexer.search("hello", 10)
    print(f"Search in {time.time() - start:.4f}s")
    print(f"Results: {results}")
    
    assert len(results) == 1
    assert results[0][1] > 0.0 # Score > 0

    print("Adding another document...")
    indexer.add_document("Hello Rust", '{"title": "Test Doc 2"}')
    
    results = indexer.search("hello", 10)
    print(f"Results for 'hello': {results}")
    assert len(results) == 2

    print("NRT Test Passed!")

if __name__ == "__main__":
    test_nrt()
