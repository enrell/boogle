// Server-side API calls (used in loaders)
import { API_URL } from "./config";
import type { 
  BookMetadata, 
  SearchResponse, 
  BookSearchResult, 
  SearchResult,
  SearchMeta,
  SearchEnhancements,
  Author,
  SourceInfo,
  FileInfo,
} from "./types";

export type { 
  BookMetadata, 
  SearchResponse, 
  BookSearchResult,
  SearchResult,
  SearchMeta,
  SearchEnhancements,
  Author,
  SourceInfo,
  FileInfo,
};

// Search filter options interface
export interface SearchFilters {
  // Provider filters
  sources?: string[];           // Include only these providers (e.g., "gutenberg", "openlibrary", "pportal")
  exclude_sources?: string[];   // Exclude these providers
  
  // Content filters
  languages?: string[];       // ISO 639-1 codes (e.g., "en", "pt", "es")
  formats?: string[];           // Available formats ("txt", "epub", "pdf", "html")
  has_fulltext?: boolean;       // Only books with downloadable full text
  
  // Quality filters
  min_completeness?: number;    // Minimum metadata completeness (0.0-1.0)
  min_rating?: number;          // Minimum rating (0.0-5.0)
  
  // Temporal filters
  year_from?: number;           // Minimum publication year (1000-2100)
  year_to?: number;             // Maximum publication year (1000-2100)
  
  // Subject filters
  subjects?: string[];          // Filter by subjects/categories
  subject_mode?: "any" | "all" | "exact";  // Subject matching mode
  
  // Result filters
  deduplicate?: boolean;        // Remove duplicate books across providers (default: true)
}

export async function getBookMetadata(canonicalId: string): Promise<BookMetadata> {
  const response = await fetch(`${API_URL}/book/${encodeURIComponent(canonicalId)}`);
  if (!response.ok) {
    if (response.status === 404) {
      throw new Error("Book not found");
    }
    throw new Error(`Failed to fetch book: ${response.statusText}`);
  }
  return response.json();
}

export async function searchBooks(
  query: string, 
  page: number = 1, 
  filters?: SearchFilters
): Promise<SearchResult[]> {
  const response = await searchBooksDetailed(query, page, filters);
  
  // Convert new API format to legacy SearchResult format for backward compatibility
  return response.results.map(book => ({
    book_id: book.canonical_id,
    title: book.title,
    author: book.authors.map(a => a.name).join(", ") || "Unknown",
    score: book.score,
    url: book.primary_source.url,
  }));
}

// Build search URL with all filter parameters
function buildSearchUrl(
  query: string,
  limit: number,
  offset: number,
  filters?: SearchFilters
): string {
  const params = new URLSearchParams();
  
  // Basic params
  params.append("query", query);
  params.append("limit", limit.toString());
  params.append("offset", offset.toString());
  
  if (!filters) {
    return `${API_URL}/search?${params.toString()}`;
  }
  
  // Provider filters
  filters.sources?.forEach(source => params.append("sources", source));
  filters.exclude_sources?.forEach(source => params.append("exclude_sources", source));
  
  // Content filters
  filters.languages?.forEach(lang => params.append("languages", lang));
  filters.formats?.forEach(format => params.append("formats", format));
  if (filters.has_fulltext !== undefined) {
    params.append("has_fulltext", filters.has_fulltext.toString());
  }
  
  // Quality filters
  if (filters.min_completeness !== undefined) {
    params.append("min_completeness", filters.min_completeness.toString());
  }
  if (filters.min_rating !== undefined) {
    params.append("min_rating", filters.min_rating.toString());
  }
  
  // Temporal filters
  if (filters.year_from !== undefined) {
    params.append("year_from", filters.year_from.toString());
  }
  if (filters.year_to !== undefined) {
    params.append("year_to", filters.year_to.toString());
  }
  
  // Subject filters
  filters.subjects?.forEach(subject => params.append("subjects", subject));
  if (filters.subject_mode) {
    params.append("subject_mode", filters.subject_mode);
  }
  
  // Result filters
  if (filters.deduplicate !== undefined) {
    params.append("deduplicate", filters.deduplicate.toString());
  }
  
  return `${API_URL}/search?${params.toString()}`;
}

// New function that returns the full search response with metadata and supports filters
export async function searchBooksDetailed(
  query: string, 
  page: number = 1,
  filters?: SearchFilters
): Promise<SearchResponse> {
  const limit = 20;
  const offset = (page - 1) * limit;
  const url = buildSearchUrl(query, limit, offset, filters);
  
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Search failed: ${response.statusText}`);
  }
  return response.json();
}
