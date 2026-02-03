"""
Tests for Open Library local database integration.
"""
import unittest
import sqlite3
import json
import os
import time
from src.enrichment.openlibrary import OpenLibraryClient, EnrichedMetadata
from src.enrichment.schema import init_db

class TestOpenLibraryLocal(unittest.TestCase):
    def setUp(self):
        self.test_db = "data/test_openlibrary.db"
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
        init_db(self.test_db)
        
        # Insert sample data
        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()
        
        sample_work = (
            "OL123W",
            "The Great Gatsby",
            json.dumps(["OL1A"]),
            4.5,
            100,
            5000,
            20,
            json.dumps(["Classic", "Fiction"])
        )
        
        cursor.execute("""
        INSERT INTO works (key, title, authors, ratings_average, ratings_count, want_to_read_count, edition_count, subjects)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, sample_work)
        
        # Force FTS rebuild if trigger didn't fire immediately (though it should)
        cursor.execute("INSERT INTO works_fts(works_fts) VALUES('rebuild')")
        
        conn.commit()
        conn.close()
        
        self.client = OpenLibraryClient(db_path=self.test_db)

    def tearDown(self):
        if os.path.exists(self.test_db):
            os.remove(self.test_db)

    def test_enrich_book_found(self):
        # Debug: print what's in the DB
        conn = sqlite3.connect(self.test_db)
        c = conn.cursor()
        c.execute("SELECT * FROM works_fts")
        print(f"\nFTS Content: {c.fetchall()}")
        conn.close()

        meta = self.client.enrich_book("The Great Gatsby", "F. Scott Fitzgerald")
        self.assertIsNotNone(meta, "Metadata should not be None")
        self.assertEqual(meta.ratings_average, 4.5)
        self.assertEqual(meta.ratings_count, 100)
        self.assertEqual(meta.want_to_read_count, 5000)
        self.assertEqual(meta.edition_count, 20)
        self.assertEqual(meta.subjects, ["Classic", "Fiction"])

    def test_enrich_book_not_found(self):
        meta = self.client.enrich_book("Nonexistent Book", "Nobody")
        self.assertIsNone(meta)

    def test_popularity_score(self):
        meta = EnrichedMetadata(
            ratings_average=5.0,
            ratings_count=100,
            want_to_read_count=10000,
            edition_count=100
        )
        score = meta.popularity_score()
        self.assertAlmostEqual(score, 1.6)

if __name__ == '__main__':
    unittest.main()
