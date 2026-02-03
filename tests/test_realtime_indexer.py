"""Tests for the RealTimeIndexer (hybrid disk + RAM index with WAL)."""
import json
import os
import shutil
import tempfile
import pytest
from pathlib import Path

from rust_bm25 import RealTimeIndexer, index_corpus_file


def create_empty_index_json(index_dir: str) -> None:
    """Create a minimal valid index.json for an empty index."""
    index_json = {
        "segments": [],
        "total_docs": 0,
        "avgdl": 0.0
    }
    with open(os.path.join(index_dir, "index.json"), "w") as f:
        json.dump(index_json, f)


class TestRealTimeIndexer:
    """Test suite for RealTimeIndexer functionality."""

    @pytest.fixture
    def index_dir(self):
        """Create a temporary directory for the index."""
        tmpdir = tempfile.mkdtemp(prefix="boogle_realtime_test_")
        yield tmpdir
        shutil.rmtree(tmpdir, ignore_errors=True)

    @pytest.fixture
    def seeded_index_dir(self, index_dir):
        """Create an index directory with some pre-indexed documents on disk."""
        books_dir = os.path.join(index_dir, "books")
        chunks_dir = os.path.join(index_dir, "chunks")
        os.makedirs(books_dir, exist_ok=True)
        os.makedirs(chunks_dir, exist_ok=True)
        
        # Create test book files
        books = [
            ("1.txt", "The quick brown fox jumps over the lazy dog. " * 20),
            ("2.txt", "Liberty and justice for all citizens of the nation. " * 20),
            ("3.txt", "Python programming language is widely used for data science. " * 20),
        ]
        
        for filename, content in books:
            with open(os.path.join(books_dir, filename), "w") as f:
                f.write(content)
        
        # Build the disk index using the batch indexer
        index_corpus_file(books_dir, index_dir, chunks_dir, [])
        
        yield index_dir

    def test_create_empty_indexer(self, index_dir):
        """Test creating a RealTimeIndexer on an empty directory."""
        create_empty_index_json(index_dir)
        
        indexer = RealTimeIndexer(index_dir)
        assert indexer is not None

    def test_add_document(self, index_dir):
        """Test adding a document to the in-memory index."""
        create_empty_index_json(index_dir)
        
        indexer = RealTimeIndexer(index_dir)
        
        doc_id = indexer.add_document(
            "This is a test document about machine learning.",
            '{"book_id": "test1", "title": "ML Guide"}'
        )
        
        assert doc_id >= 0

    def test_search_memory_only(self, index_dir):
        """Test searching documents that exist only in memory."""
        create_empty_index_json(index_dir)
        
        indexer = RealTimeIndexer(index_dir)
        
        # Add documents to memory
        indexer.add_document(
            "The quick brown fox jumps over the lazy dog.",
            '{"book_id": "1", "title": "Fox Story"}'
        )
        indexer.add_document(
            "Liberty and freedom are fundamental rights.",
            '{"book_id": "2", "title": "Rights"}'
        )
        
        # Search for "liberty"
        results = indexer.search("liberty", 5)
        
        assert len(results) > 0
        # Results should come from RAM (RAM_BOOK marker)
        assert any("RAM_BOOK" in r[0] for r in results)

    def test_search_disk_and_memory(self, seeded_index_dir):
        """Test federated search across disk segments and memory buffer."""
        indexer = RealTimeIndexer(seeded_index_dir)
        
        # Add a new document to memory that matches "liberty"
        indexer.add_document(
            "Liberty bells ring for freedom and independence.",
            '{"book_id": "new1", "title": "Liberty Bells"}'
        )
        
        # Search should find results from both disk and memory
        results = indexer.search("liberty", 10)
        
        assert len(results) > 0
        # Should have at least one result from memory
        has_memory_result = any("RAM_BOOK" in r[0] for r in results)
        assert has_memory_result, "Expected at least one result from memory buffer"

    def test_search_ranking(self, index_dir):
        """Test that search results are ranked by relevance."""
        create_empty_index_json(index_dir)
        
        indexer = RealTimeIndexer(index_dir)
        
        # Add documents with varying relevance to "python"
        indexer.add_document(
            "Python is a great programming language. Python is easy to learn.",
            '{"book_id": "1"}'
        )
        indexer.add_document(
            "Java and Python are both popular languages.",
            '{"book_id": "2"}'
        )
        indexer.add_document(
            "The snake called python is not venomous.",
            '{"book_id": "3"}'
        )
        
        results = indexer.search("python programming", 5)
        
        assert len(results) >= 2
        # First result should have highest score
        assert results[0][1] >= results[1][1]

    def test_flush_clears_memory(self, index_dir):
        """Test that flush clears the memory buffer."""
        create_empty_index_json(index_dir)
        
        indexer = RealTimeIndexer(index_dir)
        
        # Add documents
        indexer.add_document("Test document one.", '{"book_id": "1"}')
        indexer.add_document("Test document two.", '{"book_id": "2"}')
        
        # Verify documents are searchable
        results_before = indexer.search("test document", 5)
        assert len(results_before) > 0
        
        # Flush
        count = indexer.flush()
        assert count == 2
        
        # After flush, memory should be empty (but disk unchanged)
        # Note: flush doesn't write to disk in current implementation,
        # it just clears memory - so searching for memory-only docs should fail
        results_after = indexer.search("test document", 5)
        # Results should now be empty since docs were only in memory
        memory_results = [r for r in results_after if "RAM_BOOK" in r[0]]
        assert len(memory_results) == 0

    def test_wal_recovery(self, index_dir):
        """Test that WAL recovers documents after restart."""
        create_empty_index_json(index_dir)
        
        # First indexer instance - add documents
        indexer1 = RealTimeIndexer(index_dir)
        indexer1.add_document(
            "Important document that must survive crash.",
            '{"book_id": "important1"}'
        )
        indexer1.add_document(
            "Another critical document for recovery testing.",
            '{"book_id": "important2"}'
        )
        
        # Simulate crash by dropping indexer without flush
        del indexer1
        
        # New indexer instance - should recover from WAL
        indexer2 = RealTimeIndexer(index_dir)
        
        # Search for recovered documents
        results = indexer2.search("important document", 5)
        
        assert len(results) > 0, "WAL recovery should restore documents"

    def test_empty_query(self, index_dir):
        """Test that empty query returns empty results."""
        create_empty_index_json(index_dir)
        
        indexer = RealTimeIndexer(index_dir)
        indexer.add_document("Some content here.", '{"book_id": "1"}')
        
        results = indexer.search("", 5)
        assert len(results) == 0

    def test_top_k_limit(self, index_dir):
        """Test that top_k correctly limits results."""
        create_empty_index_json(index_dir)
        
        indexer = RealTimeIndexer(index_dir)
        
        # Add many documents
        for i in range(10):
            indexer.add_document(f"Document {i} about testing.", f'{{"book_id": "{i}"}}')
        
        # Request only 3 results
        results = indexer.search("testing", 3)
        
        assert len(results) <= 3
