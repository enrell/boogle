"""
Open Library Enrichment
-----------------------
Fetches enriched metadata from a local SQLite copy of Open Library data 
to improve ranking with social signals.
"""

import sqlite3
import json
import logging
import os
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class EnrichedMetadata:
    """Metadata from Open Library"""
    ratings_average: Optional[float] = None
    ratings_count: Optional[int] = None
    want_to_read_count: Optional[int] = None
    edition_count: Optional[int] = None
    subjects: list[str] = None
    
    def popularity_score(self) -> float:
        """
        Calculate a popularity score from metadata signals.
        Returns a multiplier between 1.0 and 2.0.
        """
        score = 1.0
        
        if self.ratings_average and self.ratings_count:
            if self.ratings_count >= 10:  # Minimum threshold
                rating_boost = (self.ratings_average / 5.0) * 0.3
                score += rating_boost
        
        # Want-to-read boost (up to +0.2)
        if self.want_to_read_count:
            # Logarithmic scale: 100 reads = +0.1, 1000 reads = +0.15, 10000+ = +0.2
            import math
            want_boost = min(0.2, math.log10(max(1, self.want_to_read_count)) / 20)
            score += want_boost
        
        if self.edition_count:
            edition_boost = min(0.1, self.edition_count / 100)
            score += edition_boost
            
        return min(2.0, score)


class OpenLibraryClient:
    """
    Client for Open Library enrichment using local SQLite dump.
    Replaces the API client to avoid rate limits and network latency.
    """
    
    def __init__(self, db_path: str = "data/openlibrary.db"):
        self.db_path = db_path
        
    def _get_connection(self):
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Open Library database not found at {self.db_path}. Run 'python3 scripts/manage_dumps.py' first.")
            
        try:
            return sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        except sqlite3.OperationalError:
            # Fallback for standard connection if URI fails
            return sqlite3.connect(self.db_path)
            
    def enrich_book(self, title: str, author: str) -> Optional[EnrichedMetadata]:
        """
        Look up a book in the local database using FTS.
        """
        if not title:
            return None
            
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Simple FTS query
            # We strip special chars that might break FTS syntax
            clean_title = "".join(c for c in title if c.isalnum() or c.isspace())
            clean_author = "".join(c for c in author if c.isalnum() or c.isspace()) if author else ""
            
            query_str = f'"{clean_title}"'
            # Author names are not currently in the works dump (only keys), so we can't reliably FTS them yet.
            # if clean_author and clean_author != "Unknown":
            #    query_str += f' AND "{clean_author}"'
            
            # Use FTS to find best match
            # Order by rank is implicit in FTS, but we can also check for exact title match in results
            cursor.execute("""
                SELECT 
                    ratings_average, 
                    ratings_count, 
                    want_to_read_count, 
                    edition_count, 
                    subjects 
                FROM works_fts 
                JOIN works ON works_fts.rowid = works.rowid
                WHERE works_fts MATCH ? 
                ORDER BY rank 
                LIMIT 1
            """, (query_str,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                subjects_json = row[4]
                subjects = json.loads(subjects_json) if subjects_json else []
                
                return EnrichedMetadata(
                    ratings_average=row[0],
                    ratings_count=row[1],
                    want_to_read_count=row[2],
                    edition_count=row[3],
                    subjects=subjects
                )
                
        except Exception as e:
            logger.debug(f"Open Library lookup failed for '{title}': {e}")
            
        return None
