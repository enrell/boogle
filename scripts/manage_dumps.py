"""
Script to manage Open Library data dumps: download, process, and update.
"""
import os
import sys
import gzip
import json
import logging
import sqlite3
import requests
import shutil
from pathlib import Path
from datetime import datetime
from typing import Generator, Dict, Any

# Add project root to path
sys.path.append(".")

from src.enrichment.schema import init_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
DUMP_URL_BASE = "https://openlibrary.org/data/ol_dump_works_latest.txt.gz"
DUMP_DIR = "data/dumps"
DB_PATH = "data/openlibrary.db"
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for download

def download_dump(force: bool = False) -> str:
    """Download the latest Works dump if not already present."""
    Path(DUMP_DIR).mkdir(parents=True, exist_ok=True)
    
    # Check for existing dump file
    files = list(Path(DUMP_DIR).glob("ol_dump_works_*.txt.gz"))
    if files and not force:
        latest_dump = sorted(files)[-1]
        logger.info(f"Using existing dump: {latest_dump}")
        return str(latest_dump)
    
    # Clean old dumps
    for f in files:
        f.unlink()
        
    timestamp = datetime.now().strftime("%Y-%m-%d")
    filename = f"ol_dump_works_{timestamp}.txt.gz"
    output_path = os.path.join(DUMP_DIR, filename)
    
    logger.info(f"Downloading latest works dump to {output_path}...")
    
    try:
        with requests.get(DUMP_URL_BASE, stream=True) as r:
            r.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
        logger.info("Download complete")
        return output_path
    except Exception as e:
        logger.error(f"Download failed: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
        raise

def process_dump(dump_path: str):
    """Process the dump file and populate the SQLite database."""
    init_db(DB_PATH)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = WAL")
    
    logger.info(f"Processing dump file: {dump_path}")
    
    count = 0
    batch = []
    batch_size = 10000
    
    try:
        with gzip.open(dump_path, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    # Format: type \t key \t revision \t last_modified \t JSON
                    parts = line.split('\t')
                    if len(parts) < 5:
                        continue
                        
                    # We only care about works
                    if parts[0] != '/type/work':
                        continue
                        
                    data = json.loads(parts[4])
                    
                    # Extract relevant fields
                    key = parts[1].split('/')[-1] # Remove /works/ prefix if present
                    title = data.get('title', '')
                    
                    # Authors
                    authors = []
                    for author_role in data.get('authors', []):
                        if 'author' in author_role and 'key' in author_role['author']:
                            authors.append(author_role['author']['key'])
                    
                    # Extract rating/popularity signals if present in main record
                    # Note: Detailed ratings are usually in a separate dump, but we'll take what's here
                    # Some dumps include derived fields or we might calculate them later
                    
                    # Use available fields from the work record
                    record = (
                        key,
                        title,
                        json.dumps(authors),
                        # Ratings not always in work dump, setting defaults or extracting if available
                        None, # ratings_average
                        None, # ratings_count
                        # Use direct property access with defaults
                        0, # want_to_read_count - often needs separate processing
                        len(data.get('editions', [])), # edition_count estimate
                        json.dumps(data.get('subjects', [])[:10])
                    )
                    
                    batch.append(record)
                    count += 1
                    
                    if len(batch) >= batch_size:
                        cursor.executemany("""
                        INSERT OR REPLACE INTO works 
                        (key, title, authors, ratings_average, ratings_count, want_to_read_count, edition_count, subjects)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, batch)
                        conn.commit()
                        batch = []
                        if count % 100000 == 0:
                            logger.info(f"Processed {count} records...")
                            
                except Exception as e:
                    continue
                    
        # Final batch
        if batch:
            cursor.executemany("""
            INSERT OR REPLACE INTO works 
            (key, title, authors, ratings_average, ratings_count, want_to_read_count, edition_count, subjects)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            
        logger.info(f"Finished processing {count} records")
        
    except Exception as e:
        logger.error(f"Error processing dump: {e}")
        raise
    finally:
        conn.close()

def clean_old_dumps(keep_latest: int = 1):
    """Remove old dump files to save space."""
    files = sorted(Path(DUMP_DIR).glob("ol_dump_works_*.txt.gz"))
    if len(files) > keep_latest:
        for f in files[:-keep_latest]:
            logger.info(f"Removing old dump: {f}")
            f.unlink()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Manage Open Library data dumps")
    parser.add_argument("--download-only", action="store_true", help="Only download the dump")
    parser.add_argument("--process-only", help="Process a specific dump file")
    parser.add_argument("--force", action="store_true", help="Force new download")
    
    args = parser.parse_args()
    
    try:
            if args.process_only:
                process_dump(args.process_only)
            else:
                # Check if we have a recent dump (less than 7 days old)
                should_download = args.force
                dump_path = None
                
                if not should_download:
                    files = sorted(Path(DUMP_DIR).glob("ol_dump_works_*.txt.gz"))
                    if files:
                        latest_file = files[-1]
                        file_time = datetime.fromtimestamp(latest_file.stat().st_mtime)
                        age = (datetime.now() - file_time).days
                        if age > 7:
                            logger.info(f"Existing dump is {age} days old. Downloading update...")
                            should_download = True
                        else:
                            logger.info(f"Existing dump is recent ({age} days old). Using {latest_file}.")
                            dump_path = str(latest_file)
                    else:
                        should_download = True
                
                if should_download:
                    dump_path = download_dump(force=True)
                
                if not args.download_only and dump_path:
                    process_dump(dump_path)
                    clean_old_dumps()
                
    except KeyboardInterrupt:
        logger.info("Operation cancelled")
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
