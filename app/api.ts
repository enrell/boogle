const API_BASE_URL = import.meta.env.VITE_REACT_APP_API_URL;

export interface BookMetadata {
  book_id: string;
  url: string;
  title: string | null;
  author: string | null;
  illustrator: string | null;
  release_date: string | null;
  language: string | null;
  category: string | null;
  original_publication: string | null;
  credits: string | null;
  copyright_status: string | null;
  downloads: string | null;
  files: Array<{
    format: string;
    url: string;
  }>;
}

export interface SearchResult {
  book_id: string;
  title: string;
  author: string;
  score: number;
  url: string;
}

export async function getBookMetadata(bookId: string): Promise<BookMetadata> {
  const response = await fetch(`${API_BASE_URL}/metadata/gutenberg/${bookId}`);
  if (!response.ok) {
    throw new Error(`Book not found: ${response.statusText}`);
  }
  return response.json();
}

export async function searchBooks(query: string): Promise<SearchResult[]> {
  const response = await fetch(`${API_BASE_URL}/search?query=${encodeURIComponent(query)}&limit=20`);
  if (!response.ok) {
    throw new Error(`Search failed: ${response.statusText}`);
  }
  const results = await response.json();
  if (!results || results.length === 0) {
    return [];
  }
  return results;
}
