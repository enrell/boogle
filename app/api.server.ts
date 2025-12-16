// Server-side API calls (used in loaders)
import { API_URL } from "./config";
import type { BookMetadata, SearchResult } from "./types";

export type { BookMetadata, SearchResult };

export async function getBookMetadata(bookId: string): Promise<BookMetadata> {
  const response = await fetch(`${API_URL}/metadata/gutenberg/${bookId}`);
  if (!response.ok) {
    throw new Error(`Book not found`);
  }
  return response.json();
}

export async function searchBooks(query: string, page: number = 1): Promise<SearchResult[]> {
  const limit = 20;
  const offset = (page - 1) * limit;
  const response = await fetch(`${API_URL}/search?query=${encodeURIComponent(query)}&limit=${limit}&offset=${offset}`);
  if (!response.ok) {
    throw new Error(`Search failed: ${response.statusText}`);
  }
  return (await response.json()) || [];
}
