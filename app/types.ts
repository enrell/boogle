// API v2.1.0 Types

export interface Author {
  name: string;
  role: string; // e.g., "author", "editor"
}

export interface FileInfo {
  format: string;
  url: string;
}

export interface SourceInfo {
  provider: string;
  book_id: string;
  url: string;
  files: FileInfo[];
}

export interface BookSearchResult {
  canonical_id: string;
  title: string;
  subtitle: string | null;
  authors: Author[];
  language: string | null;
  subjects: string[] | null;
  publication_year: number | null;
  cover_url: string | null;
  thumbnail_url: string | null;
  metadata_completeness: number;
  primary_source: SourceInfo;
  all_sources: SourceInfo[];
  source_count: number;
  score: number;
}

export interface SearchMeta {
  total: number;
  limit: number;
  offset: number;
  query: string;
  original_query: string;
}

export interface SearchEnhancements {
  language_detected: string | null;
  spell_corrected: boolean;
  query_expanded: boolean;
  spell_corrections: Record<string, string> | null;
  expansions: Record<string, string[]> | null;
}

export interface SearchResponse {
  results: BookSearchResult[];
  meta: SearchMeta;
  enhancements: SearchEnhancements;
}

// Full book metadata (for /book/{canonical_id})
export interface BookMetadata {
  canonical_id: string;
  title: string;
  subtitle: string | null;
  authors: Author[];
  language: string | null;
  subjects: string[] | null;
  publication_year: number | null;
  description: string | null;
  cover_url: string | null;
  thumbnail_url: string | null;
  page_count: number | null;
  isbn_10: string | null;
  isbn_13: string | null;
  primary_source: SourceInfo;
  all_sources: SourceInfo[];
  source_count: number;
  metadata_completeness: number;
  indexed_at: string;
}

// Legacy types for backward compatibility during migration
export interface SearchResult {
  book_id: string;
  title: string;
  author: string;
  score: number;
  url: string;
}
