import requests
from bs4 import BeautifulSoup
from typing import Dict, Optional, List
import re


class GutenbergScraper:
    def __init__(self, book_id: Optional[int] = None):
        self.base_url = "https://www.gutenberg.org"
        self.book_id = book_id

    def fetch(self, url: str) -> str:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text

    def get_book_url(self, book_id: int) -> str:
        return f"{self.base_url}/ebooks/{book_id}"

    def extract_metadata(self, book_id: int) -> Dict:
        url = self.get_book_url(book_id)
        html = self.fetch(url)
        soup = BeautifulSoup(html, 'html.parser')
        
        metadata = {
            'book_id': book_id,
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
                    href = link.get('href', '')
                    text = link.get_text(strip=True)
                    if href:
                        if href.startswith('http'):
                            full_url = href
                        else:
                            full_url = f"{self.base_url}{href}"
                        metadata['files'].append({
                            'format': text,
                            'url': full_url
                        })

        return metadata

    def search_books(self, query: str, limit: int = 10) -> List[Dict]:
        search_url = f"{self.base_url}/ebooks/search/?query={query}&submit_search=Go%21"
        html = self.fetch(search_url)
        soup = BeautifulSoup(html, 'html.parser')
        
        results = []
        book_links = soup.find_all('a', href=re.compile(r'/ebooks/(\d+)'))
        
        seen_ids = set()
        for link in book_links[:limit]:
            href = link.get('href', '')
            match = re.search(r'/ebooks/(\d+)', href)
            if match:
                book_id = int(match.group(1))
                if book_id not in seen_ids:
                    seen_ids.add(book_id)
                    results.append({
                        'book_id': book_id,
                        'title': link.get_text(strip=True),
                        'url': f"{self.base_url}{href}"
                    })
        
        return results
