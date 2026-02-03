"""
Open Library API Integration
----------------------------
Fetches enriched metadata from Open Library to improve ranking with social signals.
"""

import requests
import time
from typing import Optional, Dict
from dataclasses import dataclass
import logging

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
    """Client for Open Library API"""
    
    BASE_URL = "https://openlibrary.org"
    
    def __init__(self, cache_dir: str = "data/metadata_cache"):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Boogle/1.0 (Educational Search Engine)"
        })
        # Rate limiting: Open Library allows ~100 req/min
        self.last_request_time = 0
        self.min_request_interval = 0.6  # seconds
        
    def _rate_limit(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()
        
    def search_by_title_author(self, title: str, author: str) -> Optional[str]:
        """
        Search for a book and return its Open Library work ID (OLID).
        """
        try:
            self._rate_limit()
            
            query = f"title:{title}"
            if author and author != "Unknown":
                author_clean = author.split(",")[0].strip()
                query += f" author:{author_clean}"
                
            params = {
                "q": query,
                "limit": 1,
                "fields": "key"
            }
            
            resp = self.session.get(
                f"{self.BASE_URL}/search.json",
                params=params,
                timeout=5
            )
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("docs"):
                work_key = data["docs"][0].get("key")
                if work_key:
                    # Extract OLID from /works/OL123W
                    return work_key.split("/")[-1]
                    
        except Exception as e:
            logger.debug(f"Open Library search failed for '{title}': {e}")
            
        return None
        
    def get_work_metadata(self, olid: str) -> Optional[EnrichedMetadata]:
        """
        Fetch detailed metadata for a work by OLID.
        """
        try:
            self._rate_limit()
            
            resp = self.session.get(
                f"{self.BASE_URL}/works/{olid}.json",
                timeout=5
            )
            resp.raise_for_status()
            work_data = resp.json()
            
            # Fetch ratings
            ratings_resp = self.session.get(
                f"{self.BASE_URL}/works/{olid}/ratings.json",
                timeout=5
            )
            ratings_data = ratings_resp.json() if ratings_resp.status_code == 200 else {}
            
            return EnrichedMetadata(
                ratings_average=ratings_data.get("summary", {}).get("average"),
                ratings_count=ratings_data.get("summary", {}).get("count"),
                want_to_read_count=work_data.get("readinglog_count"),
                edition_count=len(work_data.get("editions", [])) if "editions" in work_data else None,
                subjects=work_data.get("subjects", [])[:5]  # Top 5 subjects
            )
            
        except Exception as e:
            logger.debug(f"Failed to fetch work metadata for {olid}: {e}")
            
        return None
        
    def enrich_book(self, title: str, author: str) -> Optional[EnrichedMetadata]:
        """
        Full enrichment pipeline: search + fetch metadata.
        """
        olid = self.search_by_title_author(title, author)
        if not olid:
            return None
            
        return self.get_work_metadata(olid)
