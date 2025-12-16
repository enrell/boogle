// Server-side API calls (used in loaders)

const API_URL = "http://localhost:8000";

export interface BookMetadata {
  source: string;
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
  files: Array<{ format: string; url: string }>;
}

export interface SearchResult {
  book_id: string;
  title: string;
  author: string;
  score: number;
  url: string;
}

export async function getBookMetadata(bookId: string): Promise<BookMetadata> {
  const response = await fetch(`${API_URL}/metadata/gutenberg/${bookId}`);
  if (!response.ok) {
    throw new Error(`Book not found`);
  }
  return response.json();
}

export async function searchBooks(query: string): Promise<SearchResult[]> {
  const response = await fetch(`${API_URL}/search?query=${encodeURIComponent(query)}&limit=20`);
  if (!response.ok) {
    throw new Error(`Search failed: ${response.statusText}`);
  }
  return (await response.json()) || [];
}
