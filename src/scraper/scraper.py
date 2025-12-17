import csv
import re
from typing import Dict, Iterator, List

import requests
from bs4 import BeautifulSoup


class GutenbergScraper:
    def __init__(self):
        self.base_url = "https://www.gutenberg.org"

    def fetch(self, url: str) -> str:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text

    def get_book_url(self, book_id: str) -> str:
        return f"{self.base_url}/ebooks/{book_id}"

    def extract_metadata(self, book_id: str) -> Dict[str, object]:
        url = self.get_book_url(book_id)
        html = self.fetch(url)
        soup = BeautifulSoup(html, 'html.parser')
        
        metadata = {
            'source': 'gutenberg',
            'book_id': str(book_id),
            'url': url,
            'title': None,
            'author': None,
            'illustrator': None,
            'release_date': None,
            'language': None,
            'category': None,
            'original_publication': None,
            'credits': None,
            'copyright_status': None,
            'downloads': None,
            'files': []
        }

        title_elem = soup.find('h1', {'id': 'book_title'})
        if title_elem:
            title = title_elem.get_text(strip=True)
            if ' by ' in title:
                title = title.split(' by ')[0].strip()
            metadata['title'] = title

        bibrec_table = soup.find('table', class_='bibrec')
        if bibrec_table:
            for row in bibrec_table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    key = th.get_text(strip=True).lower()
                    value = td.get_text(strip=True)
                    
                    if key == 'author':
                        author_link = td.find('a')
                        if author_link:
                            metadata['author'] = author_link.get_text(strip=True)
                        else:
                            metadata['author'] = value
                    elif key == 'illustrator':
                        illustrator_link = td.find('a')
                        if illustrator_link:
                            metadata['illustrator'] = illustrator_link.get_text(strip=True)
                        else:
                            metadata['illustrator'] = value
                    elif key == 'title':
                        metadata['title'] = value if not metadata['title'] else metadata['title']
                    elif 'release date' in key:
                        metadata['release_date'] = value
                    elif key == 'language':
                        metadata['language'] = value
                    elif key == 'category':
                        metadata['category'] = value
                    elif 'original publication' in key:
                        metadata['original_publication'] = value
                    elif key == 'credits':
                        metadata['credits'] = value
                    elif 'copyright status' in key:
                        metadata['copyright_status'] = value
                    elif key == 'downloads':
                        metadata['downloads'] = value

        files_table = soup.find('table', class_='files')
        if files_table:
            for row in files_table.find_all('tr'):
                link = row.find('a', class_='link')
                if link:
                    href_value = link.get('href') or ""
                    href = href_value[0] if isinstance(href_value, list) else str(href_value)
                    text = link.get_text(strip=True)
                    if href:
                        full_url = href if isinstance(href, str) and href.startswith('http') else f"{self.base_url}{href}"
                        metadata['files'].append({
                            'format': text,
                            'url': full_url
                        })

        return metadata

    def search_books(self, query: str, limit: int = 10) -> List[Dict[str, str]]:
        search_url = f"{self.base_url}/ebooks/search/?query={query}&submit_search=Go%21"
        html = self.fetch(search_url)
        soup = BeautifulSoup(html, 'html.parser')
        
        results = []
        book_links = soup.find_all('a', href=re.compile(r'/ebooks/(\d+)'))
        
        seen_ids = set()
        for link in book_links[:limit]:
            href_value = link.get('href') or ""
            href = href_value[0] if isinstance(href_value, list) else str(href_value)
            match = re.search(r'/ebooks/(\d+)', href)
            if match:
                book_id = match.group(1)
                if book_id not in seen_ids:
                    seen_ids.add(book_id)
                    results.append({
                        'source': 'gutenberg',
                        'book_id': str(book_id),
                        'title': link.get_text(strip=True),
                        'url': f"{self.base_url}{href}"
                    })
        
        return results

    def iter_book_metadata(self, limit: int | None = None) -> Iterator[Dict]:
        feed_url = f"{self.base_url}/cache/epub/feeds/pg_catalog.csv"
        
        # Stream the CSV to avoid loading 100MB+ into RAM
        with requests.get(feed_url, headers={'User-Agent': 'Mozilla/5.0'}, stream=True) as r:
            r.raise_for_status()
            # Decode lines on the fly
            lines = (line.decode('utf-8', errors='replace') for line in r.iter_lines())
            
            # Skip BOM if present
            try:
                first_line = next(lines)
                if first_line.startswith('\ufeff'):
                    first_line = first_line[1:]
                from itertools import chain
                lines = chain([first_line], lines)
            except StopIteration:
                return

            reader = csv.DictReader(lines)
            count = 0
            for row in reader:
                book_id = row.get("Text#")
                if not book_id:
                    continue
                    
                meta = {
                    'source': 'gutenberg',
                    'book_id': str(book_id),
                    'url': f"https://www.gutenberg.org/ebooks/{book_id}",
                    'title': row.get("Title"),
                    'author': row.get("Authors"),
                    'language': row.get("Language"),
                    'category': row.get("Subjects"),
                    'release_date': row.get("Issued"),
                    'files': []
                }
                
                yield meta
                count += 1
                if limit and count >= limit:
                    break

    def iter_book_ids(self, limit: int | None = None) -> Iterator[str]:
        for meta in self.iter_book_metadata(limit):
            yield meta['book_id']
